package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.model.IngestStatus;
import com.github.h3yow.telemetrymetricsjava.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.model.WindowStats;
import com.github.h3yow.telemetrymetricsjava.window.WindowStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WindowStore：并发上报不重不丢、乱序/迟到按事件时间归窗、窗口切换边界、
 * 可区分的拒绝原因、有界淘汰与查询上界。
 */
class WindowStoreTest {

    private static final Logger log = LoggerFactory.getLogger(WindowStoreTest.class);
    private static final long W = 1_000L;
    private static final long T0 = 10_000L;

    /** 可手动拨动时间的时钟，保证测试按事件时间确定性复现。 */
    private static MutableClock clockAt(long t) {
        return new MutableClock(t);
    }

    static final class MutableClock extends Clock {
        private volatile long millis;

        MutableClock(long millis) {
            this.millis = millis;
        }

        void set(long t) {
            this.millis = t;
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    private static MetricSample sample(String name, double value, long ts, String id) {
        return new MetricSample(name, Map.of("host", "h1"), value, ts, id);
    }

    @Test
    void concurrentIngestionNoLossNoDoubleCount() throws Exception {
        MutableClock clock = clockAt(T0 + 2_000);
        WindowStore store = new WindowStore(W, 100_000, 6, 100, 60_000, 0, clock);

        int threads = 16;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> failures = new ConcurrentLinkedQueue<>();
        // 每 100 个 id 中取 1 个制造重复上报（不同线程重复提交同一 id）
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        int logical = (tid * perThread + i) % (threads * perThread / 10);
                        String id = "s-" + logical;
                        var r = store.ingest(sample("cpu", 1.0, T0 + 100, id));
                        if (r.status() != IngestStatus.ACCEPTED && r.status() != IngestStatus.DUPLICATE) {
                            failures.add(r.status() + ":" + r.reason());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(failures.isEmpty(), "unexpected rejections: " + failures);

        var stats = store.latestStats(new SeriesKey("cpu", Map.of("host", "h1"))).orElseThrow();
        int uniqueIds = threads * perThread / 10;
        log.info("concurrent result: submitted={} unique={} counted={} sum={}",
                threads * perThread, uniqueIds, stats.count(), stats.sum());
        assertEquals(uniqueIds, stats.count(), "each unique sampleId counted exactly once");
        assertEquals(uniqueIds, stats.sum(), 1e-9);
    }

    @Test
    void outOfOrderSamplesLandInEventTimeWindowsWithoutDoubleCountAcrossBoundary() {
        MutableClock clock = clockAt(T0 + 2_500);
        WindowStore store = new WindowStore(W, 10_000, 6, 100, 60_000, 0, clock);
        // 乱序到达：先到 w1，再到 w0；边界样本 1999 属 w1、2000 属 w2
        long[][] input = {{T0 + 1_500, 5}, {T0 + 200, 1}, {T0 + 1_999, 9}, {T0 + 2_000, 7}, {T0 + 200, 1}};
        String[] ids = {"w1a", "w0a", "w1b", "w2a", "w0a"};
        for (int i = 0; i < input.length; i++) {
            var r = store.ingest(sample("cpu", input[i][1], input[i][0], ids[i]));
            log.info("input ts={} v={} id={} -> {} window={}", input[i][0], input[i][1], ids[i],
                    r.status(), r.windowStartMs());
        }
        SeriesKey key = new SeriesKey("cpu", Map.of("host", "h1"));
        List<WindowStats> all = store.statsBetween(key, T0, T0 + 2 * W);
        all.forEach(s -> log.info("window [{},{}) count={} sum={} min={} max={}",
                s.windowStartMs(), s.windowEndMs(), s.count(), s.sum(), s.min(), s.max()));
        assertEquals(3, all.size());
        assertEquals(1, all.get(0).count());
        assertEquals(2, all.get(1).count(), "1500 and 1999 both belong to window 1000");
        assertEquals(1, all.get(2).count());
        assertEquals(14.0, all.get(1).sum(), 1e-9, "5+9; duplicate id w0a not counted twice");
    }

    @Test
    void lateSamplesRejectedAfterWindowClosesAndReasonIsDistinguishable() {
        MutableClock clock = clockAt(T0 + 500);
        WindowStore store = new WindowStore(W, 10_000, 6, 100, 60_000, 0, clock);
        var first = store.ingest(sample("cpu", 1, T0 + 100, "a"));
        assertEquals(IngestStatus.ACCEPTED, first.status());

        // 推进到 T0+2001：w0=[T0,T0+1000) 已关闭（lateness=0）
        clock.set(T0 + 2_001);
        var sealedEvents = store.advanceWatermark(clock.millis());
        assertEquals(1, sealedEvents.size(), "w0 seals exactly once");
        log.info("sealed window start={} count={}", sealedEvents.get(0).stats().windowStartMs(),
                sealedEvents.get(0).stats().count());

        var late = store.ingest(sample("cpu", 2, T0 + 200, "late"));
        log.info("late sample -> {} reason={}", late.status(), late.reason());
        assertEquals(IngestStatus.REJECTED_TOO_OLD, late.status());

        // 再次推进不会重复关闭
        assertEquals(0, store.advanceWatermark(clock.millis() + 1).size());

        // 允许乱序的迟到：lateness=500 时，now=T0+1400 仍接受 w0 样本
        WindowStore lenient = new WindowStore(W, 10_000, 6, 100, 60_000, 500, clock);
        var ok = lenient.ingest(sample("cpu", 3, T0 + 200, "b"));
        assertEquals(IngestStatus.ACCEPTED, ok.status());
    }

    @Test
    void rejectsFutureFullWindowAndTooManySeriesWithDistinctReasons() {
        MutableClock clock = clockAt(T0);
        WindowStore store = new WindowStore(W, 2, 6, 5, 5_000, 0, clock);

        var future = store.ingest(sample("cpu", 1, T0 + 50_000, "f"));
        assertEquals(IngestStatus.REJECTED_TOO_FUTURE, future.status());
        log.info("future reason: {}", future.reason());

        store.ingest(sample("cpu", 1, T0 + 10, "a"));
        store.ingest(sample("cpu", 1, T0 + 11, "b"));
        var full = store.ingest(sample("cpu", 1, T0 + 12, "c"));
        assertEquals(IngestStatus.REJECTED_WINDOW_FULL, full.status());
        log.info("full reason: {}", full.reason());

        AtomicReference<IngestStatus> cap = new AtomicReference<>();
        for (int i = 0; i < 10; i++) {
            var r = store.ingest(new MetricSample("cpu", Map.of("host", "h" + i), 1.0, T0 + 10, "x" + i));
            if (r.status() != IngestStatus.ACCEPTED) {
                cap.set(r.status());
            }
        }
        assertEquals(IngestStatus.REJECTED_TOO_MANY_SERIES, cap.get());
        log.info("series cap status: {}", cap.get());
    }

    @Test
    void retentionIsBoundedAndQueriesSeeOnlyRetainedWindows() {
        MutableClock clock = clockAt(T0 + 100);
        int retained = 3;
        WindowStore store = new WindowStore(W, 10_000, retained, 100, 60_000, 0, clock);
        SeriesKey key = new SeriesKey("cpu", Map.of("host", "h1"));

        // 写 5 个窗口后滚动关闭；每序列最多保留 3 个（含活跃窗口）
        for (int i = 0; i < 5; i++) {
            store.ingest(sample("cpu", i + 1, T0 + i * W + 10, "id" + i));
        }
        clock.set(T0 + 5 * W + 1);
        var sealed = store.advanceWatermark(clock.millis());
        log.info("sealed {} windows; retained window starts queried:", sealed.size());
        List<WindowStats> remaining = store.statsBetween(key, 0, Long.MAX_VALUE);
        remaining.forEach(s -> log.info("  retained [{},{})", s.windowStartMs(), s.windowEndMs()));
        assertEquals(retained, remaining.size(), "retention caps windows per series");
        assertEquals(T0 + 2 * W, remaining.get(0).windowStartMs());

        // 被淘汰窗口的迟到样本按 TOO_OLD 拒绝（窗口已不存在）
        var ancient = store.ingest(sample("cpu", 1, T0 + 10, "old"));
        assertEquals(IngestStatus.REJECTED_TOO_OLD, ancient.status());
        assertFalse(store.latestStats(key).isEmpty());
    }
}
