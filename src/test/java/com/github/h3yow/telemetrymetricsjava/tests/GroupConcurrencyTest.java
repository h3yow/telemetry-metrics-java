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
        return newService(clock, 0);
    }

    private static MetricsService newService(MutableClock clock, long graceMs) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(6);
        p.setLateArrivalGraceMs(graceMs);
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

    /**
     * 强断言并发滚动：多个 host 在多个窗口内的水位按“触发→死区→恢复→重新触发”编排，
     * 写线程与维护线程激烈交错。无论封定批次以什么线程调度顺序到达判定器，
     * 每个（分组×窗口）都必须恰好判定一次：最终翻转事件集合与顺序参考实现完全一致
     * （不漏判、不重复判），且事件按窗口升序到达。
     */
    @Test
    void concurrentRollingNeverSkipsNorDuplicatesWindowAcrossRepeatedRuns() throws Exception {
        // 期望的水位编排（每窗口每组一个样本，MAX 判定，fire=80 / recover=50）
        double[] aValues = {90, 95, 95, 40, 45, 90}; // 触发 w0；死区 w1/w2；恢复 w3；死区 w4；重触发 w5
        double[] bValues = {10, 10, 90, 90, 90, 10}; // 触发 w2；w3/w4 仍 FIRING；恢复 w5
        final double[] groupA = aValues;
        final double[] groupB = bValues;
        // 样本落在窗口 0..5；期望翻转（绝对窗口起点）
        java.util.List<String> expected = java.util.List.of(
                (0 * 1_000L) + "#host=a#FIRING",
                (2 * 1_000L) + "#host=b#FIRING",
                (3 * 1_000L) + "#host=a#RESOLVED",
                (5 * 1_000L) + "#host=a#FIRING",
                (5 * 1_000L) + "#host=b#RESOLVED");

        int runs = 30;
        for (int run = 0; run < runs; run++) {
            final int runId = run;
            long t0 = 60_000L + run * 100_000L;
            MutableClock clock = new MutableClock(t0);
            // 30s 迟到宽限：窗口只在其结束+宽限后封定，保证并发上报期间样本不被判 TOO_OLD；
            // 阶段二时钟一次性推到全部窗口之后再驱动封定。
            MetricsService service = newService(clock, 30_000);
            service.registerGroupRule(new GroupAlertRule("grp-exact", "cpu", List.of("host"),
                    com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule.Aggregator.MAX,
                    80.0, 50.0));
            AtomicInteger accepted = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();

            // 阶段一：6 个写线程在固定时钟下并发上报全部 12 个样本（ingest 路径仍会并发
            // 触发维护），不会有窗口越过“结束+宽限”的封定点。
            clock.setTime(t0 + 3_000L);
            List<int[]> tasks = new ArrayList<>();
            for (int w = 0; w < 6; w++) {
                tasks.add(new int[]{0, w});
                tasks.add(new int[]{1, w});
            }
            java.util.Collections.shuffle(tasks, new java.util.Random(run));
            int writerThreads = 6;
            ExecutorService writers = Executors.newFixedThreadPool(writerThreads);
            CountDownLatch writeStart = new CountDownLatch(1);
            int perWriter = tasks.size() / writerThreads;
            for (int t = 0; t < writerThreads; t++) {
                final List<int[]> mine = tasks.subList(t * perWriter,
                        t == writerThreads - 1 ? tasks.size() : (t + 1) * perWriter);
                writers.submit(() -> {
                    try {
                        writeStart.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int[] tw : mine) {
                        String host = tw[0] == 0 ? "a" : "b";
                        double value = tw[0] == 0 ? groupA[tw[1]] : groupB[tw[1]];
                        IngestResult r = service.ingest(new MetricSample("cpu", value,
                                t0 + tw[1] * 1_000L + 500, Map.of("host", host),
                                "run" + runId + "-" + host + "-w" + tw[1]));
                        if (r.accepted()) {
                            accepted.incrementAndGet();
                        } else {
                            rejected.incrementAndGet();
                        }
                    }
                });
            }
            writeStart.countDown();
            writers.shutdown();
            assertTrue(writers.awaitTermination(30, TimeUnit.SECONDS));
            assertEquals(12, accepted.get(), () -> "全部样本应被接受，run=" + runId
                    + " rejected=" + rejected.get());

            // 阶段二：两个维护线程并发把时钟从 t0+3s 小步推过全部窗口的封定点
            // （最后窗口 w5 结束 t0+6s + 宽限 30s = t0+36s），激烈竞争封定临界区、
            // 投递闸门与判定器锁。事件只能在这一阶段产生。
            ExecutorService rollers = Executors.newFixedThreadPool(2);
            CountDownLatch rollStart = new CountDownLatch(1);
            for (int rt = 0; rt < 2; rt++) {
                rollers.submit(() -> {
                    try {
                        rollStart.await();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int step = 0; step < 68; step++) {
                        clock.advanceMs(500);
                        service.maintain();
                        Thread.yield();
                    }
                });
            }
            rollStart.countDown();
            rollers.shutdown();
            assertTrue(rollers.awaitTermination(30, TimeUnit.SECONDS));

            assertEquals(12, accepted.get() + rejected.get(), "每个样本必须有唯一确定去向");
            assertEquals(12, accepted.get(), () -> "不应有拒绝，run=" + runId);

            // 维护线程最终时钟约为 t0+37s（已越过 w5 封定点 t0+36s）；补一次维护兜底
            if (clock.millis() < t0 + 37_000L) {
                clock.setTime(t0 + 37_000L);
            }
            service.maintain();

            List<GroupAlertEvent> events = service.drainGroupAlertEvents();
            List<String> actual = events.stream()
                    .filter(e -> e.ruleId().equals("grp-exact"))
                    .map(e -> (e.windowStart() - t0) + "#" + e.groupKey() + "#" + e.state())
                    .toList();

            // 1) 事件窗口必须单调不减（按事件时间顺序产出，绝不被乱序投递打乱）
            long prevWindow = Long.MIN_VALUE;
            for (GroupAlertEvent e : events) {
                assertTrue(e.windowStart() >= prevWindow,
                        "事件必须按窗口升序产出，run=" + runId + " events=" + actual);
                prevWindow = e.windowStart();
            }
            // 2) 集合与顺序参考完全一致：不漏（a 在 w3 的恢复等）、不重
            assertEquals(expected, actual, "run=" + run + " 判定结果必须与顺序参考一致");
            // 3) 重复 tick 不产生任何额外事件
            for (int i = 0; i < 3; i++) {
                clock.advanceMs(1_000);
                service.maintain();
            }
            assertTrue(service.drainGroupAlertEvents().isEmpty(),
                    "重复滚动不得重复判定，run=" + run);
        }
        log.info("[分组并发-恰好一次] {} 轮并发滚动全部得到与顺序参考一致的 5 个翻转事件", runs);
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
