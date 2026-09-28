package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
 * 1) 多线程向多个 host 分组并发上报：组内样本数 = 各序列 count 之和，不重复不漏；
 * 2) 上报与窗口滚动（封定/批量分组判定）并发：每分组每窗口至多产生一次 FIRING，
 *    事件总数有界且可解释；
 * 3) 相同输入重复运行，分组聚合结果一致（确定性）。
 */
class GroupConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(GroupConcurrencyTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(6);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(60_000);
        p.setMaxSamplesPerSeriesWindow(1_000_000);
        p.setMaxGroupCombinations(50);
        p.setMaxSeriesPerGroup(50);
        return new MetricsService(p, clock);
    }

    @Test
    void concurrentGroupedIngestCountsEverySampleOnce() throws Exception {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        int threads = 8;
        int hosts = 4;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        ConcurrentLinkedQueue<IngestResult> rejects = new ConcurrentLinkedQueue<>();
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    int seq = base + i;
                    String host = "h" + (seq % hosts);
                    String svc = "s" + (seq % 7); // 不参与分组，只制造更多序列
                    IngestResult r = service.ingest(new MetricSample("cpu", 1.0,
                            t0 + 100 + (seq % 800), Map.of("host", host, "service", svc),
                            "g-" + seq));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    } else {
                        rejects.add(r);
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        log.info("[分组并发-计数] 输入={} 接受={} 拒绝={}", threads * perThread, accepted.get(), rejects.size());
        assertEquals(threads * perThread, accepted.get());
        assertTrue(rejects.isEmpty(), () -> "不应有拒绝: " + rejects);

        // 分组聚合：4 个组的样本总数必须等于接受总数（不重复不漏）
        List<GroupWindowAggregation> groups =
                service.queryGrouped("cpu", List.of("host"), t0, t0);
        assertEquals(4, groups.size());
        long groupSamples = groups.stream().mapToLong(GroupWindowAggregation::sampleCount).sum();
        long groupSeries = groups.stream().mapToLong(GroupWindowAggregation::seriesCount).sum();
        log.info("[分组并发-计数] 各组样本数={} 序列数={} 总样本={}",
                groups.stream().map(g -> g.groupLabels() + "=" + g.sampleCount() + "/" + g.seriesCount()).toList(),
                groupSeries, groupSamples);
        assertEquals(threads * perThread, groupSamples, "跨序列分组合计不得重复或遗漏");
        // 每台机器的所有 service 序列应被合并进同一个 host 组
        assertTrue(groups.stream().allMatch(g -> g.seriesCount() == 7),
                "每个 host 组应包含全部 7 个 service 序列，而不是被 service 拆开");

        // 与逐序列查询的总数交叉校验
        long seriesCountTotal = 0L;
        for (String h : new String[]{"h0", "h1", "h2", "h3"}) {
            for (int s = 0; s < 7; s++) {
                seriesCountTotal += service.query("cpu", Map.of("host", h, "service", "s" + s), t0, t0)
                        .stream().mapToLong(WindowAggregation::count).sum();
            }
        }
        assertEquals(threads * perThread, seriesCountTotal, "逐序列总数也应一致");
    }

    @Test
    void groupedAlertFiresAtMostOncePerGroupPerWindowUnderConcurrentRolling() throws Exception {
        long t0 = 80_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        // 按 host 分组、SUM 判定，固定阈值，结果与到达顺序无关
        service.registerGroupRule(new GroupAlertRule("grp-roll", "cpu", List.of("host"),
                com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule.Aggregator.SUM, 1_000.0, 0.0));

        int writerThreads = 6;
        int windowsCovered = 4;
        int hosts = 3;
        int perWindowPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(writerThreads + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();

        for (int t = 0; t < writerThreads; t++) {
            final int base = t;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int w = 0; w < windowsCovered; w++) {
                    for (int i = 0; i < perWindowPerThread; i++) {
                        int seq = base * 100_000 + w * 10_000 + i;
                        MetricSample sm = new MetricSample("cpu", 1.0,
                                t0 + w * 1_000L + 100 + (i % 800),
                                Map.of("host", "h" + (seq % hosts)),
                                "roll-" + seq);
                        IngestResult r = service.ingest(sm);
                        if (r.accepted()) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                }
            });
        }
        // 维护线程：推进时钟并封定/批量判定（不 sleep，尽量与写线程激烈竞争）
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            for (int step = 0; step < 20; step++) {
                clock.advanceMs(250);
                service.maintain();
                Thread.yield();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // 每个样本确定去向
        int total = writerThreads * windowsCovered * perWindowPerThread;
        log.info("[分组并发-滚动] 输入={} 接受={} 拒绝={}", total, accepted.get(), rejected.get());
        assertEquals(total, accepted.get() + rejected.get());

        // 全部窗口再维护一次，确保都已封定判定
        clock.setTime(t0 + windowsCovered * 1_000L + 1);
        service.maintain();

        List<GroupAlertEvent> events = service.drainGroupAlertEvents();
        long fireKeys = events.stream()
                .filter(e -> e.ruleId().equals("grp-roll"))
                .map(e -> e.groupKey() + "@" + e.windowStart())
                .distinct().count();
        log.info("[分组并发-滚动] 分组告警事件数={} 去重(分组×窗口)数={}", events.size(), fireKeys);
        assertEquals(fireKeys, events.size(), "同一分组同一窗口只能产生一次事件");

        // 再补几次维护，不得产生任何重复事件
        for (int i = 0; i < 3; i++) {
            clock.advanceMs(1_000);
            service.maintain();
        }
        assertTrue(service.drainGroupAlertEvents().isEmpty(), "窗口滚动重复 tick 不得重复判定");
    }

    @Test
    void sameInputRepeatedlyYieldsIdenticalGroupedResult() {
        List<GroupWindowAggregation> first = runOnce(90_000L);
        for (int run = 0; run < 5; run++) {
            List<GroupWindowAggregation> again = runOnce(90_000L);
            assertEquals(summarize(first), summarize(again),
                    "重复运行同一批输入，分组结果必须一致");
        }
        log.info("[分组并发-确定] 重复运行 6 次结果一致: {}", summarize(first));
    }

    private static List<GroupWindowAggregation> runOnce(long t0) {
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        List<MetricSample> batch = new ArrayList<>();
        int seq = 0;
        // 故意打乱 host/service 顺序，验证结果不依赖到达顺序
        for (int w = 0; w < 3; w++) {
            for (int s = 0; s < 5; s++) {
                for (int h = 0; h < 3; h++) {
                    batch.add(new MetricSample("cpu", (h + 1) * (w + 1),
                            t0 + w * 1_000L + 100 + s,
                            Map.of("host", "h" + h, "service", "s" + s, "pod", "p" + (seq % 13)),
                            "d-" + (seq++)));
                }
            }
        }
        for (MetricSample sm : batch) {
            assertTrue(service.ingest(sm).accepted());
        }
        return service.queryGrouped("cpu", List.of("host"), t0, t0 + 2_000);
    }

    private static List<String> summarize(List<GroupWindowAggregation> rows) {
        return rows.stream()
                .map(g -> g.windowStart() + "|" + g.groupLabels() + "|samples=" + g.sampleCount()
                        + "|series=" + g.seriesCount() + "|sum=" + g.sum())
                .toList();
    }
}
