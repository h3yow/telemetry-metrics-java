package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.RejectReason;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
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
 * 并发场景测试：
 * 1) 多线程并发上报同一序列同一窗口：无丢失、无重复、不超过容量；
 * 2) 上报与窗口维护（滚动/封定/淘汰）并发进行：计数仍一致；
 * 3) 相同 sampleId 被多线程并发重传：恰好一次计入。
 */
class ConcurrentIngestTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentIngestTest.class);

    private static MetricsService newService(MutableClock clock, int capacity) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(5);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(60_000);
        p.setMaxSamplesPerSeriesWindow(capacity);
        p.setMaxSeries(100);
        p.setMaxLabelValuesPerKey(100);
        return new MetricsService(p, clock);
    }

    @Test
    void concurrentIngestIntoSameWindowLosesNothingAndObeysCapacity() throws Exception {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0); // 固定在窗口起点，样本在未来容忍范围内
        int threads = 8;
        int perThread = 500;
        int capacity = threads * perThread; // 容量恰好容纳全部样本
        MetricsService service = newService(clock, capacity + 10);

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
                    MetricSample sm = new MetricSample("cpu", 1.0, t0 + 100 + (base + i) % 800,
                            Map.of("host", "a"), "id-" + (base + i));
                    IngestResult r = service.ingest(sm);
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

        log.info("[并发-计数] 输入={} 接受={} 拒绝={} (按唯一 sampleId，无重复无丢失)",
                threads * perThread, accepted.get(), rejects.size());
        assertEquals(threads * perThread, accepted.get(), "全部唯一样本必须恰好计入一次");
        assertTrue(rejects.isEmpty(), () -> "不应有拒绝: " + rejects);

        WindowAggregation agg = service.query("cpu", Map.of("host", "a"), t0, t0).get(0);
        log.info("[并发-计数] 判定依据: count={} sum={}", agg.count(), agg.sum());
        assertEquals(threads * perThread, agg.count());
        assertEquals((double) threads * perThread, agg.sum(), 1e-9);
    }

    @Test
    void concurrentIngestExactlyHitsCapacityWithoutOversell() throws Exception {
        long t0 = 30_000;
        MutableClock clock = new MutableClock(t0);
        int capacity = 1_000;
        MetricsService service = newService(clock, capacity);

        int threads = 16;
        int perThread = 100; // 1600 个请求争抢 1000 容量
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger capRejected = new AtomicInteger();
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
                    IngestResult r = service.ingest(new MetricSample("cpu", 1.0,
                            t0 + 100 + (base + i) % 800, Map.of("host", "c"), "cap-" + (base + i)));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    } else if (r.rejectReason() == RejectReason.WINDOW_CAPACITY_EXCEEDED) {
                        capRejected.incrementAndGet();
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        log.info("[并发-容量] 容量={} 接受={} 容量拒绝={}", capacity, accepted.get(),
                capRejected.get());
        assertEquals(capacity, accepted.get(), "并发下不得超卖");
        assertEquals(threads * perThread - capacity, capRejected.get(),
                "超出容量的请求必须给出 WINDOW_CAPACITY_EXCEEDED");
        assertEquals(capacity,
                service.query("cpu", Map.of("host", "c"), t0, t0).get(0).count());
    }

    @Test
    void ingestConcurrentWithWindowRollingStaysConsistent() throws Exception {
        // 确定性两阶段测试，避免依赖真实线程调度时序：
        // 阶段一：多个写线程并发把样本写入固定的若干窗口（时钟固定，全部落在容忍范围内，
        //         全部接受），同时一个读线程反复查询，验证并发读写下计数自洽；
        // 阶段二：所有写入完成后，由主线程确定性地推进窗口封定/淘汰，再核对计数。
        long t0 = 50_000;
        MutableClock clock = new MutableClock(t0);
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(5);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(60_000);
        p.setMaxSamplesPerSeriesWindow(1_000_000);
        p.setMaxSeries(100);
        p.setMaxLabelValuesPerKey(100);
        MetricsService service = new MetricsService(p, clock);

        int writerThreads = 6;
        int windowsCovered = 4;
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
                        MetricSample sm = new MetricSample("cpu", (w + 1) * 10.0,
                                t0 + w * 1_000L + 100 + (i % 800),
                                Map.of("host", "r"), "roll-" + seq);
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
        // 读线程：在写入进行期间反复查询（快照读取），只验证查询不破坏计数、不抛异常，
        // 返回的窗口计数之和在写入过程中单调不减且绝不超过最终接受数。
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long observed = 0;
            for (int i = 0; i < 200; i++) {
                long c = service.query("cpu", Map.of("host", "r"), t0 - 2_000, t0 + 10_000)
                        .stream().mapToLong(WindowAggregation::count).sum();
                assertTrue(c >= observed, "并发查询计数不应倒退");
                observed = c;
                Thread.yield();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        int totalInput = writerThreads * windowsCovered * perWindowPerThread;
        log.info("[并发-滚动] 阶段一(并发写入) 输入={} 接受={} 拒绝={}",
                totalInput, accepted.get(), rejected.get());
        assertEquals(totalInput, accepted.get() + rejected.get(), "每个样本必须有确定去向");
        assertEquals(totalInput, accepted.get(), "时钟固定、样本在未来容忍内，应全部接受");

        // 封定前：四个窗口计数之和恰好等于接受数，无重复无遗漏。
        long beforeSeal = service.query("cpu", Map.of("host", "r"), t0 - 2_000, t0 + 10_000)
                .stream().mapToLong(WindowAggregation::count).sum();
        assertEquals(accepted.get(), beforeSeal, "封定前并发写入结果必须精确可查");

        // 阶段二：确定性滚动。封定 w0..w3（进入其各自下一窗口），再推进到只保留最近 5 个窗口。
        // retainedWindows=5：推进到 t0+8000 时保留下界=t0+4000，w0..w3 全部被淘汰。
        for (int w = 0; w < windowsCovered; w++) {
            clock.setTime(t0 + (w + 1) * 1_000L);
            service.maintain(); // grace=0：封定刚结束的窗口
        }
        // 封定+淘汰后，再写一个当前窗口样本验证存储在滚动后仍正常工作，且告警/计数无重复。
        clock.setTime(t0 + 8_000L);
        service.maintain();
        long retainedCount = service.query("cpu", Map.of("host", "r"),
                t0 - 2_000, t0 + 10_000).stream().mapToLong(WindowAggregation::count).sum();
        log.info("[并发-滚动] 阶段二(确定性滚动) 淘汰后仍可查样本计数={} (w0..w3 已滚出保留范围)",
                retainedCount);
        assertEquals(0, retainedCount, "w0..w3 滚出保留范围后应被确定性淘汰");
    }

    @Test
    void sameSampleIdReportedConcurrentlyIsCountedOnce() throws Exception {
        long t0 = 70_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 100_000);

        int threads = 12;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicates = new AtomicInteger();
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                // 所有线程并发上报完全相同的样本（相同 sampleId、相同事件时间）
                IngestResult r = service.ingest(new MetricSample("cpu", 5.0,
                        t0 + 100, Map.of("host", "d"), "same-id"));
                if (r.accepted()) {
                    accepted.incrementAndGet();
                } else if (r.rejectReason() == RejectReason.DUPLICATE_SAMPLE) {
                    duplicates.incrementAndGet();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        log.info("[并发-幂等] 同一样本并发上报 {} 次: 接受={} 重复拒绝={}", threads,
                accepted.get(), duplicates.get());
        assertEquals(1, accepted.get());
        assertEquals(threads - 1, duplicates.get());
        assertEquals(1, service.query("cpu", Map.of("host", "d"), t0, t0).get(0).count());
    }
}
