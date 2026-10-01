package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发上报 × 窗口滚动<b>真正交错</b>下，分组告警端到端不漏判、不重判、可复现的测试。
 *
 * <p>一个“节拍器”按真实墙钟逐窗口推进可控时钟（窗口被打开→并发写满→并发封定）；
 * 多个写线程在窗口打开期间并发上报，多个维护线程在同一时间窗口内反复 maintain，
 * 使得“封定 w(k) 与封定 w(k+1) 分别发生在不同线程、且彼此乱序竞争投递”在整个长跑中
 * 高频出现（这正是线上漏判的竞态）。对账不依赖固定样本数，而是基于<b>实际被接受</b>
 * 的样本用纯函数独立重算每个（host × 窗口）的 SUM 与滞回翻转，再与真实事件比对：</p>
 * <ul>
 *   <li>漏一个翻转 / 多一个翻转 / 同一（host×窗口）重复翻转都判失败；</li>
 *   <li>重复跑多次，最终事件签名完全一致（可复现）；</li>
 *   <li>长跑后状态条目、事件队列有界，空窗口滚动延迟稳定。</li>
 * </ul>
 */
class GroupAlertConcurrentSoakTest {

    private static final Logger log = LoggerFactory.getLogger(GroupAlertConcurrentSoakTest.class);

    private static final long SIZE = 1_000;
    private static final int HOSTS = 3;
    private static final int WINDOWS = 8;
    private static final int WRITERS = 6;
    private static final int MAINTAINERS = 4;
    private static final int PER_WINDOW_PER_WRITER = 40;

    // fire/recover 阈值。偶数窗每 host 样本数 = 2 线程 * 40 = 80 条，值约 10 -> SUM≈800；
    // 奇数窗值约 0.1 -> SUM≈8，确保偶数必越线、奇数必恢复，且不依赖具体到达顺序。
    private static final double FIRE = 200.0;
    private static final double RECOVER = 50.0;

    private static MetricsService newService(MutableClock clock, int retentionWindows) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(SIZE);
        p.setRetainedWindows(40);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(120_000);
        p.setMaxSamplesPerSeriesWindow(1_000_000);
        p.setMaxSeries(10_000);
        p.setMaxLabelValuesPerKey(10_000);
        p.setMaxGroupCombinations(100);
        p.setMaxSeriesPerGroup(100);
        p.setGroupAlertRetentionWindows(retentionWindows);
        MetricsService service = new MetricsService(p, clock);
        service.registerGroupRule(new GroupAlertRule("soak", "cpu", List.of("host"),
                AlertRule.Aggregator.SUM, FIRE, RECOVER));
        return service;
    }

    private static double valueOf(int w, int t, int i) {
        double base = (w % 2 == 0) ? 10.0 : 0.1;
        return base + (t % 3) * 0.0001 + (i % 7) * 0.00001;
    }

    /** "host#window" -> 该组该窗被接受样本的值之和（用于独立对账）。 */
    private final Map<String, AtomicLong> acceptCount = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> acceptSumX1000 = new ConcurrentHashMap<>();

    private String key(int host, int w) {
        return host + "#" + w;
    }

    private record RunResult(List<String> eventSignatures, long events,
                             long accepted, long rejected, long statusCount) { }

    private RunResult runOnce(long t0, int runIdx) throws Exception {
        acceptCount.clear();
        acceptSumX1000.clear();
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 10_000);

        int threads = WRITERS + MAINTAINERS + 1;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        // 节拍器完成标志：维护线程据此退出，避免依赖时钟条件造成忙等活锁。
        java.util.concurrent.atomic.AtomicBoolean pacingDone =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        // 每窗口两道闸：open[w] 放开写 w；written[w] 等所有写线程写完 w，节拍器才封定 w。
        // 这样样本一定在封定前计入（不会 TOO_OLD），而封定动作与下一窗口写入、与多个
        // 维护线程的 maintain 在多线程间真实交错、乱序竞争。
        CountDownLatch[] open = new CountDownLatch[WINDOWS];
        CountDownLatch[] written = new CountDownLatch[WINDOWS];
        for (int w = 0; w < WINDOWS; w++) {
            open[w] = new CountDownLatch(1);
            written[w] = new CountDownLatch(WRITERS);
        }

        // 写线程：逐窗口等待开放 -> 写满该窗口 -> 报告完成。
        for (int t = 0; t < WRITERS; t++) {
            final int thread = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int w = 0; w < WINDOWS; w++) {
                    try {
                        open[w].await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < PER_WINDOW_PER_WRITER; i++) {
                        int host = (thread * 31 + w * 7 + i) % HOSTS;
                        double v = valueOf(w, thread, i);
                        IngestResult r = service.ingest(new MetricSample("cpu", v,
                                t0 + w * SIZE + 100 + (i % 800),
                                Map.of("host", "h" + host),
                                "soak-" + runIdx + "-" + thread + "-" + w + "-" + i));
                        if (r.accepted()) {
                            accepted.incrementAndGet();
                            acceptCount.computeIfAbsent(key(host, w),
                                    k -> new AtomicLong()).incrementAndGet();
                            acceptSumX1000.computeIfAbsent(key(host, w), k -> new AtomicLong())
                                    .addAndGet(Math.round(v * 1000));
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                    written[w].countDown();
                }
            });
        }

        // 维护线程：全程高频 maintain，与节拍器、写线程并发竞争封定与批量投递。
        for (int m = 0; m < MAINTAINERS; m++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int iter = 0; iter < 500_000 && !pacingDone.get(); iter++) {
                    service.maintain();
                    Thread.yield();
                }
                service.maintain();
            });
        }

        // 节拍器：开放 w -> 等所有写线程写完 w -> 推进时钟到 w+1 起点并封定 w（不等
        // 维护线程），随后立刻开放 w+1。封定 w 的批次可能由节拍器或某个维护线程在
        // 不同线程、不同先后顺序投递，正是要压的竞态。
        AtomicReference<Throwable> pacerFailure = new AtomicReference<>();
        pool.submit(() -> {
            try {
                start.await();
                for (int w = 0; w < WINDOWS; w++) {
                    clock.setTime(t0 + w * SIZE + 10);
                    open[w].countDown();
                    written[w].await();
                    clock.setTime(t0 + (w + 1) * SIZE);
                    service.maintain();
                }
                clock.setTime(t0 + (WINDOWS + 2) * SIZE);
                service.maintain();
            } catch (Throwable t) {
                pacerFailure.set(t);
            } finally {
                pacingDone.set(true);
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "线程池应正常结束");
        if (pacerFailure.get() != null) {
            throw new AssertionError("节拍器异常", pacerFailure.get());
        }

        // 最终兜底：把时钟推到全部窗口封定之后，确保残留窗口都判定完（写得晚的样本
        // 若越过封定点会被记为拒绝并从对账输入中剔除，不影响正确性）。
        clock.setTime(t0 + (WINDOWS + 2) * SIZE);
        service.maintain();

        List<GroupAlertEvent> events = service.drainGroupAlertEvents();
        List<String> sigs = events.stream()
                .map(e -> e.groupKey() + "|" + e.state() + "@" + e.windowStart())
                .sorted().toList();
        long statusCount = service.groupAlertStatuses().size();
        log.info("[并发soak] run={} 接受={} 拒绝={} 事件={} 状态条目={}",
                runIdx, accepted.get(), rejected.get(), events.size(), statusCount);
        return new RunResult(new ArrayList<>(sigs), events.size(),
                accepted.get(), rejected.get(), statusCount);
    }

    /** 基于实际接受样本独立重算期望翻转签名（纯函数滞回状态机，按 host 独立、按窗口升序）。 */
    private List<String> expectedFromAccepted(long t0) {
        List<String> out = new ArrayList<>();
        for (int h = 0; h < HOSTS; h++) {
            boolean firing = false;
            for (int w = 0; w < WINDOWS; w++) {
                AtomicLong sumX = acceptSumX1000.get(key(h, w));
                AtomicLong cnt = acceptCount.get(key(h, w));
                if (sumX == null || cnt.get() == 0) {
                    continue; // 该组该窗无接受样本：不推进状态
                }
                double sum = sumX.get() / 1000.0;
                if (!firing && sum >= FIRE) {
                    firing = true;
                    out.add("host=h" + h + "|FIRING@" + (t0 + w * SIZE));
                } else if (firing && sum <= RECOVER) {
                    firing = false;
                    out.add("host=h" + h + "|RESOLVED@" + (t0 + w * SIZE));
                }
            }
        }
        out.sort(null);
        return out;
    }

    @Test
    void interleavedIngestAndRollingNeverSkipsDuplicatesOrReordersAndIsReproducible() throws Exception {
        long t0 = 300_000;
        RunResult r1 = runOnce(t0, 0);
        List<String> expected1 = expectedFromAccepted(t0);

        log.info("[并发soak] run0 期望翻转={} 实际事件={}", expected1.size(), r1.events());
        // 对账：实际事件集合必须恰好等于“按接受样本独立重算”的期望（不漏/不重/不乱）。
        assertEquals(expected1, r1.eventSignatures(),
                "交错并发下事件必须与独立重算一致（漏判/重判/乱序都失败）");

        // 同批输入重复跑两次：节拍相位、线程调度不同，最终事件签名必须一致。
        RunResult r2 = runOnce(t0, 1);
        RunResult r3 = runOnce(t0, 2);
        assertEquals(r1.eventSignatures(), r2.eventSignatures(), "重复跑结果必须一致");
        assertEquals(r1.eventSignatures(), r3.eventSignatures(), "重复跑结果必须一致");

        // 状态条目有界：恰为出现过的 host 组数。
        assertEquals(HOSTS, r1.statusCount(), "分组状态条目数有界（=分组数）");
        // 每个样本都在窗口封定前计入（节拍器已等全部写完才封定），因此必须全部接受。
        long totalInputs = WRITERS * WINDOWS * PER_WINDOW_PER_WRITER;
        assertEquals(totalInputs, r1.accepted() + r1.rejected(),
                "每个样本必须有确定去向（接受或拒绝）");
        assertEquals(0, r1.rejected(), "封定前已写完的样本不应被判 TOO_OLD");
    }

    @Test
    void longEmptyRunKeepsStatusAndLatencyBounded() {
        long t0 = 600_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 3);
        long startNs = System.nanoTime();
        for (int w = 0; w < 3_000; w++) {
            clock.setTime(t0 + (w + 2) * SIZE);
            service.maintain();
        }
        long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
        assertTrue(service.groupAlertStatuses().isEmpty());
        assertTrue(service.drainGroupAlertEvents().isEmpty());
        log.info("[并发soak] 滚动 3000 个空窗口耗时={}ms（延迟稳定，不随时间劣化）", elapsedMs);
        assertTrue(elapsedMs < 10_000, "空窗口滚动应有界且快速，实际 " + elapsedMs + "ms");
    }
}
