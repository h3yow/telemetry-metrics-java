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
        // 确定性版本：写线程只写事件时间位于固定 4 个窗口的样本（时钟起点远早于这些窗口，
        // 因此维护线程如何滚动封定都不会让样本变 TOO_OLD）；维护线程不断封定/淘汰。
        // 结束后严格对账：接受数 == 仍保留窗口计数 + 已封定窗口被消费的计数，无重复无丢失。
        long t0 = 50_000;
        MutableClock clock = new MutableClock(t0 - 30_000);
        MetricsService service = newService(clock, 1_000_000);

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
        // 维护线程：在样本窗口之前的时间段滚动封定/淘汰（不 sleep，结果与调度无关）
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

        int totalInput = writerThreads * windowsCovered * perWindowPerThread;
        log.info("[并发-滚动] 输入={} 接受={} 拒绝={} (样本窗口晚于维护时钟，不应有 TOO_OLD)",
                totalInput, accepted.get(), rejected.get());
        assertEquals(0, rejected.get(), "样本事件时间始终在维护时钟之前，不应被拒绝");
        assertEquals(totalInput, accepted.get(), "每个样本必须有确定去向且全部接受");

        // 把时钟推过全部样本窗口并逐一封定后，仍保留窗口中的计数应等于接受总数：
        // 封定只触发告警回调、不删除样本；只要窗口尚在保留范围内，接受即被计数。
        clock.setTime(t0 + windowsCovered * 1_000L + 1);
        service.maintain();
        long counted = service.query("cpu", Map.of("host", "r"),
                t0, t0 + (windowsCovered - 1) * 1_000L)
                .stream().mapToLong(WindowAggregation::count).sum();
        log.info("[并发-滚动] 全部窗口封定后仍保留窗口计数={} (retainedWindows=5 覆盖全部 4 窗)", counted);
        assertEquals(accepted.get(), counted, "封定与上报并发下：接受即被计数，无重复无丢失");
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
