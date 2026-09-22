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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 窗口聚合与拒绝原因测试：正常聚合、迟到拒绝、未来拒绝、容量拒绝、序列/标签基数拒绝。
 */
class WindowAggregationTest {

    private static final Logger log = LoggerFactory.getLogger(WindowAggregationTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(3);
        p.setLateArrivalGraceMs(500);
        p.setMaxFutureSkewMs(2_000);
        p.setMaxSamplesPerSeriesWindow(3);
        p.setMaxSeries(3);
        p.setMaxLabelValuesPerKey(3);
        return new MetricsService(p, clock);
    }

    private static MetricSample s(double value, long ts, Map<String, String> labels, String id) {
        return new MetricSample("cpu", value, ts, labels, id);
    }

    @Test
    void aggregatesByWindowAndLabelUsingEventTime() {
        long t0 = 10_000; // 与窗口边界对齐
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        Map<String, String> hostA = Map.of("host", "a");

        List<MetricSample> input = List.of(
                s(10, t0 + 100, hostA, "a1"),
                s(20, t0 + 200, hostA, "a2"),
                s(30, t0 + 1_100, hostA, "a3"), // 下一个窗口
                s(40, t0 + 100, Map.of("host", "b"), "b1") // 不同标签 -> 不同序列
        );
        log.info("[聚合] 输入样本: {}", input.stream().map(x -> x.value() + "@" + x.timestamp()
                + x.labels()).toList());

        for (MetricSample in : input) {
            IngestResult r = service.ingest(in);
            log.info("[聚合] 样本 value={} ts={} labels={} -> accepted={} windowStart={} detail={}",
                    in.value(), in.timestamp(), in.labels(), r.accepted(), r.windowStartMs(),
                    r.detail());
            assertTrue(r.accepted(), () -> "应被接受: " + r.detail());
        }

        List<WindowAggregation> w0 = service.query("cpu", hostA, t0, t0);
        assertEquals(1, w0.size());
        WindowAggregation agg = w0.get(0);
        log.info("[聚合] 判定依据 host=a 窗口[{}]: count={}, sum={}, min={}, max={}, avg={}",
                t0, agg.count(), agg.sum(), agg.min(), agg.max(), agg.average());
        assertEquals(2, agg.count());
        assertEquals(30.0, agg.sum(), 1e-9);
        assertEquals(10.0, agg.min(), 1e-9);
        assertEquals(20.0, agg.max(), 1e-9);
        assertEquals(15.0, agg.average(), 1e-9);

        List<WindowAggregation> w1 = service.query("cpu", hostA, t0 + 1_000, t0 + 1_000);
        assertEquals(1, w1.size());
        assertEquals(1, w1.get(0).count());
        assertEquals(30.0, w1.get(0).lastValue(), 1e-9);
        assertEquals(40.0, service.query("cpu", Map.of("host", "b"), t0, t0)
                .get(0).lastValue(), 1e-9);
    }

    @Test
    void rejectsWithDistinguishableReasons() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        Map<String, String> hostA = Map.of("host", "a");

        // 容量：窗口容量为 3
        for (int i = 0; i < 3; i++) {
            assertTrue(service.ingest(s(i + 1, t0 + i, hostA, "cap" + i)).accepted());
        }
        IngestResult cap = service.ingest(s(99, t0 + 900, hostA, "capX"));
        log.info("[拒绝-容量] 判定: accepted={} reason={} detail={}",
                cap.accepted(), cap.rejectReason(), cap.detail());
        assertFalse(cap.accepted());
        assertEquals(RejectReason.WINDOW_CAPACITY_EXCEEDED, cap.rejectReason());

        // 过旧：时间推进后，超过迟到宽限
        clock.setTime(t0 + 2_000);
        MetricSample oldOne = s(1, t0 + 100, Map.of("host", "old"), "old1");
        IngestResult tooOld = service.ingest(oldOne);
        log.info("[拒绝-过旧] 输入 ts={}, now={} -> reason={} detail={}",
                oldOne.timestamp(), clock.millis(), tooOld.rejectReason(), tooOld.detail());
        assertFalse(tooOld.accepted());
        assertEquals(RejectReason.TOO_OLD, tooOld.rejectReason());

        // 过未来：超过 now + futureSkew(2000)
        MetricSample future = s(1, t0 + 5_000, Map.of("host", "f"), "f1");
        IngestResult tooFuture = service.ingest(future);
        log.info("[拒绝-未来] 输入 ts={}, now={} -> reason={} detail={}",
                future.timestamp(), clock.millis(), tooFuture.rejectReason(), tooFuture.detail());
        assertFalse(tooFuture.accepted());
        assertEquals(RejectReason.TOO_FAR_IN_FUTURE, tooFuture.rejectReason());

        // 序列数上限 3：当前已有 host=a / old / f 中实际入列的；用全新标签打满验证
        MetricsService fresh = newService(new MutableClock(t0));
        assertTrue(fresh.ingest(s(1, t0, Map.of("h", "1"), "x1")).accepted());
        assertTrue(fresh.ingest(s(1, t0, Map.of("h", "2"), "x2")).accepted());
        assertTrue(fresh.ingest(s(1, t0, Map.of("h", "3"), "x3")).accepted());
        IngestResult tooMany = fresh.ingest(s(1, t0, Map.of("h", "4"), "x4"));
        log.info("[拒绝-序列数] reason={} detail={}", tooMany.rejectReason(), tooMany.detail());
        assertFalse(tooMany.accepted());
        assertEquals(RejectReason.TOO_MANY_SERIES, tooMany.rejectReason());

        // 非法参数
        IngestResult invalid = service.ingest(
                new MetricSample("", 1, t0, Map.of(), null));
        log.info("[拒绝-非法] reason={} detail={}", invalid.rejectReason(), invalid.detail());
        assertEquals(RejectReason.INVALID_ARGUMENT, invalid.rejectReason());
    }

    @Test
    void windowRollingEvictsOldWindowsAndBoundsMemory() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        Map<String, String> hostA = Map.of("host", "a");

        for (int w = 0; w < 6; w++) {
            clock.setTime(t0 + w * 1_000L);
            assertTrue(service.ingest(s(w, clock.millis(), hostA, "w" + w)).accepted());
            service.maintain();
        }
        // 保留 3 个窗口：最早窗口的数据已不可查（且样本会被 TOO_OLD 拒绝）
        List<WindowAggregation> range =
                service.query("cpu", hostA, t0, t0 + 6_000);
        log.info("[滚动] 窗口推进 6 次后可查窗口起点: {}",
                range.stream().map(WindowAggregation::windowStart).toList());
        assertEquals(3, range.size(), "只应保留最近 3 个窗口");
        assertTrue(range.stream().allMatch(a -> a.windowStart() >= t0 + 3_000));

        IngestResult old = service.ingest(s(1, t0 + 50, Map.of("host", "late"), "late1"));
        log.info("[滚动] 迟到样本结论: reason={} detail={}", old.rejectReason(), old.detail());
        assertEquals(RejectReason.TOO_OLD, old.rejectReason());
    }
}
