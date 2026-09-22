package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.alert.ComparisonOp;
import com.github.h3yow.telemetrymetricsjava.alert.MetricFunction;
import com.github.h3yow.telemetrymetricsjava.config.TelemetryProperties;
import com.github.h3yow.telemetrymetricsjava.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.service.TelemetryService;
import com.github.h3yow.telemetrymetricsjava.window.WindowStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 服务编排：窗口滚动驱动告警、事件历史有界；并发上报与滚动交织时统计可复现。
 */
class TelemetryServiceTest {

    private static final Logger log = LoggerFactory.getLogger(TelemetryServiceTest.class);
    private static final long W = 1_000L;

    static final class FixedClock extends Clock {
        private volatile long t;

        FixedClock(long t) {
            this.t = t;
        }

        void set(long t) {
            this.t = t;
        }

        @Override
        public long millis() {
            return t;
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
            return Instant.ofEpochMilli(t);
        }
    }

    private static TelemetryProperties props(int history) {
        TelemetryProperties p = new TelemetryProperties();
        p.setWindowSizeMs(W);
        p.setWindowCapacity(100_000);
        p.setRetainedWindowsPerSeries(6);
        p.setMaxSeries(100);
        p.setMaxFutureSkewMs(60_000);
        p.setAllowedLatenessMs(0);
        p.setEventHistorySize(history);
        return p;
    }

    private static MetricSample s(double v, long ts, String id) {
        return new MetricSample("cpu", Map.of("host", "h1"), v, ts, id);
    }

    @Test
    void tickSealsWindowsAndProducesOnlyStateTransitions() {
        FixedClock clock = new FixedClock(0);
        WindowStore store = new WindowStore(W, 100_000, 6, 100, 60_000, 0, clock);
        com.github.h3yow.telemetrymetricsjava.alert.AlertEngine engine =
                new com.github.h3yow.telemetrymetricsjava.alert.AlertEngine(store);
        TelemetryService service = new TelemetryService(store, engine, clock, props(10));
        service.addRule(new AlertRule("r1", "cpu", Map.of(), MetricFunction.AVG, ComparisonOp.GT, 10.0));

        // w0 越线(avg=12)，w1 正常(avg=8)，w2 再次越线(avg=20)
        service.ingest(s(12, 10, "a"));
        service.tick(W); // 不关闭任何窗口（w0 endMs=1000 > watermark=1000? 1000<=1000 => 关闭）
        service.ingest(s(8, W + 10, "b"));
        service.tick(2 * W);
        service.ingest(s(20, 2 * W + 10, "c"));
        service.tick(3 * W);

        List<AlertEvent> events = service.recentEvents(50);
        log.info("produced events in time order (history newest-first):");
        events.forEach(e -> log.info("  {} rule={} window={} observed={} basis={}",
                e.kind(), e.ruleId(), e.windowStartMs(), e.observedValue(), e.thresholdMessage()));
        assertEquals(3, events.size());
        // 历史 newest-first
        assertEquals(AlertEvent.Kind.FIRING, events.get(0).kind());
        assertEquals(AlertEvent.Kind.RECOVERED, events.get(1).kind());
        assertEquals(AlertEvent.Kind.FIRING, events.get(2).kind());
    }

    @Test
    void eventHistoryIsBounded() {
        FixedClock clock = new FixedClock(0);
        WindowStore store = new WindowStore(W, 100_000, 6, 100, 60_000, 0, clock);
        var engine = new com.github.h3yow.telemetrymetricsjava.alert.AlertEngine(store);
        TelemetryService service = new TelemetryService(store, engine, clock, props(2));
        service.addRule(new AlertRule("r", "cpu", Map.of(), MetricFunction.AVG, ComparisonOp.GT, 0.0));
        for (int w = 0; w < 5; w++) {
            // 在阈值两侧交替：每个窗口都产生一次状态迁移（FIRING/RECOVERED 共 5 个事件）
            service.ingest(s(w % 2 == 0 ? 1 : -1, w * W + 1, "id" + w));
            service.tick((w + 1) * W);
        }
        assertEquals(2, service.recentEvents(100).size());
    }

    @Test
    void concurrentIngestionInterleavedWithRolloverIsReproducible() throws Exception {
        FixedClock clock = new FixedClock(3 * W);
        WindowStore store = new WindowStore(W, 1_000_000, 6, 100, 60_000, 0, clock);
        var engine = new com.github.h3yow.telemetrymetricsjava.alert.AlertEngine(store);
        TelemetryService service = new TelemetryService(store, engine, clock, props(10));

        int producers = 8;
        int perProducer = 1_000;
        int total = producers * perProducer;
        ExecutorService pool = Executors.newFixedThreadPool(producers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();

        // 生产者：事件时间均匀落在 w0..w2（0..2999），唯一 id
        for (int p = 0; p < producers; p++) {
            final int pid = p;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perProducer; i++) {
                        int seq = pid * perProducer + i;
                        long ts = (seq % (3 * W));
                        var r = service.ingest(s(1, ts, "id-" + seq));
                        if (r.status().name().equals("ACCEPTED")) {
                            accepted.incrementAndGet();
                        } else if (r.status().name().equals("DUPLICATE")) {
                            duplicate.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        // 滚动者：从 now=3s 推进到 now=6s，期间部分早期样本会变成 TOO_OLD
        pool.submit(() -> {
            try {
                start.await();
                for (long t = 3 * W; t <= 6 * W; t += 250) {
                    clock.set(t);
                    service.tick(t);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        // 不变量：被计数的样本总数可复现地等于 事件时间落在“最终未被提前关闭的窗口”中且唯一的 id 数。
        // 关键断言：accepted + duplicate + (被拒样本) 中，accepted 与窗口最终 count 之和一致，
        // 且所有已关闭窗口的 count 在滚动后不再变化（不重不漏）。
        long counted = 0;
        SeriesKey key = new SeriesKey("cpu", Map.of("host", "h1"));
        for (int w = 0; w < 6; w++) {
            var stats = service.statsBetween(key, w * W, w * W);
            counted += stats.stream().mapToLong(x -> x.count()).sum();
        }
        log.info("interleaved: submitted={} accepted={} duplicate={} countedInWindows={}",
                total, accepted.get(), duplicate.get(), counted);
        assertEquals(0, duplicate.get(), "no duplicate ids submitted");
        // 被计数样本必然全部来自 accepted；accepted 样本必然被计数（已关闭窗口的 count 也被查询保留）
        assertEquals(accepted.get(), counted,
                "accepted samples and window counts match regardless of interleaving");
        assertTrue(counted < total, "samples racing past rollover were deterministically rejected");

        // 再滚动一次，已关闭窗口统计不得变化
        long before = counted;
        clock.set(20 * W);
        service.tick(20 * W);
        long after = 0;
        for (int w = 0; w < 6; w++) {
            long ww = w;
            after += service.statsBetween(key, ww * W, ww * W).stream().mapToLong(x -> x.count()).sum();
        }
        assertEquals(before, after, "sealed window statistics are immutable");
    }
}
