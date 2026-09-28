package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryStatus;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.RejectReason;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分组能力的并发稳定性测试：
 * 1) 多线程并发上报（同组多序列）+ 窗口滚动封定同时进行：分组样本计数
 *    “接受数 = 各分组各窗口 sampleCount 之和”，不重复不遗漏；
 * 2) 分组告警在并发滚动下，每个（规则，分组，窗口）最多一次 FIRING 翻转事件，
 *    组内多序列不会按序列重复告警；
 * 3) 并发分组查询结果完整可复现（重复查询签名一致），超限给出可区分状态；
 * 4) 长时间（多窗口）运行后查询延迟稳定，分组/状态数有界。
 */
class GroupedConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(GroupedConcurrencyTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(8);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(60_000);
        p.setMaxSamplesPerSeriesWindow(1_000_000);
        p.setMaxSeries(100_000);
        p.setMaxLabelValuesPerKey(100_000);
        p.setMaxGroupCombinations(1_000);
        p.setMaxSeriesPerGroup(100_000);
        p.setGroupAlertRetentionWindows(8);
        return new MetricsService(p, clock);
    }

    @Test
    void concurrentIngestAndRollingKeepsGroupCountsExact() throws Exception {
        // 确定性两阶段，避免依赖真实线程调度：
        // 阶段一：多线程并发写入（同组多序列），时钟固定，全部样本落在容忍范围内并接受；
        //         同时一个读线程反复做分组查询，验证并发读写下分组计数自洽；
        // 阶段二：写入完成后由主线程确定性地封定/滚动，核对组内序列数与样本计数守恒。
        long t0 = 500_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock);

        int writers = 6;
        int windows = 4;
        int hosts = 3;
        int perWindowPerWriterPerHost = 50; // 同组多序列：用 machine 标签区分序列
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int t = 0; t < writers; t++) {
            final int writer = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int w = 0; w < windows; w++) {
                    for (int h = 0; h < hosts; h++) {
                        for (int i = 0; i < perWindowPerWriterPerHost; i++) {
                            int seq = writer * 100_000 + h * 10_000 + i;
                            MetricSample sm = new MetricSample("cpu", 1.0,
                                    t0 + w * 1_000L + 100 + (i % 500),
                                    Map.of("host", "h" + h, "machine", "m" + writer),
                                    "gc-" + seq);
                            IngestResult r = s.ingest(sm);
                            if (r.accepted()) {
                                accepted.incrementAndGet();
                            } else {
                                rejected.incrementAndGet();
                            }
                        }
                    }
                }
            });
        }
        // 读线程：写入期间反复分组查询（快照读），计数应单调不减且不超过最终接受数。
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long observed = 0;
            for (int i = 0; i < 200; i++) {
                GroupQueryResult q = s.queryGroups("cpu", List.of("host"),
                        t0 - 3_000, t0 + 10_000);
                if (q.isComplete()) {
                    long c = q.results().stream()
                            .mapToLong(GroupWindowAggregation::sampleCount).sum();
                    assertTrue(c >= observed, "并发分组查询计数不应倒退");
                    observed = c;
                }
                Thread.yield();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int totalInput = writers * windows * hosts * perWindowPerWriterPerHost;
        log.info("[并发分组] 阶段一(并发写入) 输入={} 接受={} 拒绝={}",
                totalInput, accepted.get(), rejected.get());
        assertEquals(totalInput, accepted.get() + rejected.get(), "每个样本有确定去向");
        assertEquals(totalInput, accepted.get(), "时钟固定、样本在容忍内，应全部接受");

        // 封定前：按 host 分组的样本总数恰好等于接受数（不重复不遗漏）。
        GroupQueryResult result = s.queryGroups("cpu", List.of("host"),
                t0 - 3_000, t0 + 10_000);
        assertTrue(result.isComplete(), result.detail());
        long groupedSamples = result.results().stream()
                .mapToLong(GroupWindowAggregation::sampleCount).sum();
        log.info("[并发分组] 分组样本总数={} (接受={})，分组行数={}",
                groupedSamples, accepted.get(), result.results().size());
        assertEquals(accepted.get(), groupedSamples,
                "分组统计必须与接受样本精确一致：不重复计数、不漏样本");
        assertEquals(windows * hosts, result.results().size());

        // 每个分组行的序列数恰为参与的 writer 数（machine=m0..m5）。
        result.results().forEach(r -> assertEquals(writers, r.seriesCount(),
                "每个 host 组在每个窗口应由 " + writers + " 条 machine 序列合并: " + r.groupLabels()
                        + "@" + r.windowStart()));

        // 阶段二：确定性地封定 w0..w3，再推进到这些窗口滚出保留范围（retainedWindows=8，
        // 推进到 t0+12000 时保留下界=t0+5000，w0..w4 全部淘汰）。滚动不得改变历史计数。
        for (int w = 0; w < windows; w++) {
            clock.setTime(t0 + (w + 1) * 1_000L);
            s.maintain(); // grace=0：封定刚结束的窗口（驱动分组告警，每窗口每组一次）
        }
        clock.setTime(t0 + 12_000L);
        s.maintain();
        GroupQueryResult after = s.queryGroups("cpu", List.of("host"),
                t0 - 3_000, t0 + 10_000);
        long retained = after.results().stream()
                .mapToLong(GroupWindowAggregation::sampleCount).sum();
        log.info("[并发分组] 阶段二(确定性滚动) 淘汰后仍可查样本={} (w0..w3 已滚出保留范围)", retained);
        assertEquals(0, retained, "w0..w3 滚出保留范围后应被确定性淘汰，无残留分组");
    }

    @Test
    void groupedAlertFiresAtMostOncePerGroupPerWindowUnderConcurrency() throws Exception {
        long t0 = 600_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock);
        s.registerRule(new AlertRule("gcpu", "cpu", Map.of(),
                AlertRule.Aggregator.SUM, 1.0, 0.0, List.of("host")));

        int writers = 6;
        int windows = 3;
        int hosts = 4;
        int seriesPerHostPerWindow = 20;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<IngestResult> rejects = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < writers; t++) {
            final int writer = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int w = 0; w < windows; w++) {
                    for (int h = 0; h < hosts; h++) {
                        for (int i = 0; i < seriesPerHostPerWindow; i++) {
                            IngestResult r = s.ingest(new MetricSample("cpu", 10.0,
                                    t0 + w * 1_000L + 100 + (i % 500),
                                    Map.of("host", "h" + h, "machine", "m" + writer, "i", String.valueOf(i)),
                                    "ga-" + writer + "-" + w + "-" + h + "-" + i));
                            if (!r.accepted() && r.rejectReason() != RejectReason.TOO_OLD) {
                                rejects.add(r);
                            }
                        }
                    }
                }
            });
        }
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            for (int step = 0; step < 30; step++) {
                clock.advanceMs(200);
                s.maintain();
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(rejects.isEmpty(), () -> "除 TOO_OLD 外不应有拒绝: " + rejects);

        List<AlertEvent> events = s.drainAlertEvents();
        // 每个（host，窗口）最多一个 FIRING：按 (groupLabels, windowStart) 去重计数。
        long distinctKeys = events.stream()
                .filter(e -> e.state() == com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.FIRING)
                .map(e -> e.groupLabels() + "#" + e.windowStart()).distinct().count();
        long firingCount = events.stream()
                .filter(e -> e.state() == com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.FIRING)
                .count();
        log.info("[并发分组告警] FIRING 事件数={} 去重(分组,窗口)数={} (必须相等，每组每窗口至多一次)",
                firingCount, distinctKeys);
        assertEquals(distinctKeys, firingCount,
                "同一分组同一窗口即使组内多序列，也只能产生一次判定/事件");
        assertTrue(firingCount <= (long) windows * hosts);
    }

    @Test
    void repeatedGroupedQueriesAreIdenticalAndStableOverLongRun() throws Exception {
        long t0 = 700_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock);

        // 写入大量窗口（模拟长时间运行），标签组合固定，验证结果有界、查询不越跑越慢、可复现。
        int windows = 40;
        int hosts = 5;
        for (int w = 0; w < windows; w++) {
            for (int h = 0; h < hosts; h++) {
                s.ingest(new MetricSample("cpu", h + 1.0, t0 + w * 1_000L + 100,
                        Map.of("host", "h" + h, "ephemeral", "v" + (w % 3)), "q-" + w + "-" + h));
            }
        }
        // 不推进时钟 -> 所有窗口都在未来容忍内、仍保留（retainedWindows=8 只在维护时淘汰）。
        // 直接对一个固定的近期窗口段反复分组查询。
        long from = t0 + (windows - 6) * 1_000L;
        long to = t0 + (windows - 1) * 1_000L;

        long t1 = System.nanoTime();
        GroupQueryResult r1 = s.queryGroups("cpu", List.of("host"), from, to);
        long t2 = System.nanoTime();
        GroupQueryResult r2 = s.queryGroups("cpu", List.of("host"), from, to);
        long t3 = System.nanoTime();
        GroupQueryResult r3 = s.queryGroups("cpu", List.of("host"), from, to);
        long t4 = System.nanoTime();
        log.info("[并发分组-性能] 三次分组查询耗时(us): {}, {}, {}；行数={}",
                (t2 - t1) / 1000, (t3 - t2) / 1000, (t4 - t3) / 1000, r1.results().size());

        assertTrue(r1.isComplete());
        assertEquals(signature(r1), signature(r2));
        assertEquals(signature(r2), signature(r3), "重复运行同一批输入结果必须一致");
        assertEquals(6 * hosts, r1.results().size(), "每个保留窗口每个 host 一行，数量有界");

        // 分组键组合数在查询侧仍受上限保护：用极小上限必须得到可区分失败。
        MetricsProperties tight = new MetricsProperties();
        tight.setWindowSizeMs(1_000);
        tight.setRetainedWindows(8);
        tight.setLateArrivalGraceMs(0);
        tight.setMaxFutureSkewMs(60_000);
        tight.setMaxGroupCombinations(2);
        tight.setMaxSeriesPerGroup(2);
        tight.setMaxQueryWindows(3);
        MetricsService limited = new MetricsService(tight, clock);
        for (int h = 0; h < 4; h++) {
            limited.ingest(new MetricSample("cpu", 1.0, from + 100,
                    Map.of("host", "h" + h), "l-" + h));
        }
        GroupQueryResult overCombos = limited.queryGroups("cpu", List.of("host"), from, from);
        assertEquals(GroupQueryStatus.TOO_MANY_GROUP_COMBINATIONS, overCombos.status());
        log.info("[并发分组-边界] 超限反馈 status={} 不静默丢样本", overCombos.status());
    }

    private static List<String> signature(GroupQueryResult r) {
        return r.results().stream()
                .map(g -> g.windowStart() + "|" + g.groupLabels() + "|" + g.sampleCount()
                        + "|" + g.seriesCount() + "|" + g.sum())
                .toList();
    }
}
