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

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序与迟到样本测试：
 * 1) 样本按事件时间归入正确窗口，跨边界不得重复计入；
 * 2) 迟到但在宽限内的样本计入封定窗口；过宽限被 TOO_OLD 拒绝；
 * 3) lastValue 按事件时间而非到达时间定义。
 */
class OutOfOrderSampleTest {

    private static final Logger log = LoggerFactory.getLogger(OutOfOrderSampleTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(3);
        p.setLateArrivalGraceMs(500);
        p.setMaxFutureSkewMs(10_000);
        return new MetricsService(p, clock);
    }

    private static MetricSample s(double value, long ts, String id) {
        return new MetricSample("temp", value, ts, Map.of("sensor", "s1"), id);
    }

    @Test
    void outOfOrderSamplesLandInEventTimeWindowsWithoutDoubleCount() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        // 时钟走到第二窗口中段（now=t0+1600），第一窗口最末样本年龄已达 550ms（>宽限 500ms）
        clock.setTime(t0 + 1_600);

        // 第一窗口样本：事件时间距 now 已超过迟到宽限(500ms) -> TOO_OLD，绝不计入任何窗口
        IngestResult old1 = service.ingest(s(100, t0 + 1_050, "late-w1-1"));
        IngestResult old2 = service.ingest(s(110, t0 + 100, "w0-1"));
        log.info("[乱序] now={} 超宽限样本 eventTs={}/{} -> reason={}/{}", clock.millis(),
                t0 + 1_050, t0 + 100, old1.rejectReason(), old2.rejectReason());        assertFalse(old1.accepted());
        assertFalse(old2.accepted());
        assertEquals(RejectReason.TOO_OLD, old1.rejectReason());
        assertEquals(RejectReason.TOO_OLD, old2.rejectReason());

        // 第二窗口样本：乱序到达（1200 后到、1500 先到的反向叙述由逐条日志体现）
        MetricSample a = s(200, t0 + 1_200, "w2-1");
        MetricSample b = s(210, t0 + 1_500, "w2-2");
        for (MetricSample in : List.of(a, b)) {
            IngestResult r = service.ingest(in);
            log.info("[乱序] value={} eventTs={} -> accepted={} 归入窗口={} detail={}",
                    in.value(), in.timestamp(), r.accepted(), r.windowStartMs(), r.detail());
            assertTrue(r.accepted(), () -> "当前窗口样本应接受: " + r.detail());
            assertEquals(t0 + 1_000, r.windowStartMs());
        }

        // 同一 sampleId 重传（模拟 at-least-once 重试）必须被幂等拒绝，而不是再计一次
        MetricSample replay = s(999, t0 + 1_200, "w2-1");
        IngestResult dup = service.ingest(replay);
        log.info("[乱序] 重复 sampleId 重报 value={} ts={} -> reason={} detail={}",
                replay.value(), replay.timestamp(), dup.rejectReason(), dup.detail());
        assertFalse(dup.accepted());
        assertEquals(RejectReason.DUPLICATE_SAMPLE, dup.rejectReason());

        // 第一窗口无任何聚合（超宽限样本既未计入本窗口也未串到别的窗口）
        assertTrue(service.query("temp", Map.of("sensor", "s1"), t0, t0).isEmpty());

        // 第二窗口 count 恰为 2，重传未计入，sum 可复现
        WindowAggregation w2 =
                service.query("temp", Map.of("sensor", "s1"), t0 + 1_000, t0 + 1_000).get(0);
        log.info("[乱序] 判定依据 第二窗口聚合: count={} sum={} min={} max={}",
                w2.count(), w2.sum(), w2.min(), w2.max());
        assertEquals(2, w2.count());
        assertEquals(410.0, w2.sum(), 1e-9);
    }

    @Test
    void lateSampleWithinGraceStillCountedOnceInItsOwnWindow() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        // 第一窗口内先到一条样本
        assertTrue(service.ingest(s(10, t0 + 100, "w0-1")).accepted());
        // 时间推进到 t0+1200：第一窗口刚结束 200ms（< 宽限 500ms），迟到样本仍应进入第一窗口
        clock.setTime(t0 + 1_200);
        MetricSample late = s(20, t0 + 900, "w0-late");
        IngestResult r = service.ingest(late);
        log.info("[迟到宽限] now={}, 迟到样本 eventTs={} -> accepted={} 窗口={} detail={}",
                clock.millis(), late.timestamp(), r.accepted(), r.windowStartMs(), r.detail());
        assertTrue(r.accepted());
        assertEquals(t0, r.windowStartMs());

        WindowAggregation w0 =
                service.query("temp", Map.of("sensor", "s1"), t0, t0).get(0);
        log.info("[迟到宽限] 判定依据 第一窗口(含迟到): count={} sum={} lastValue={}",
                w0.count(), w0.sum(), w0.lastValue());
        assertEquals(2, w0.count());
        assertEquals(30.0, w0.sum(), 1e-9);

        // lastValue 按事件时间：迟到样本事件时间更晚，尽管它更晚到达，仍应为窗口最后值
        assertEquals(20.0, w0.lastValue(), 1e-9);

        // 再过 400ms，第一窗口年龄超宽限，迟到样本被拒，统计不再变化
        clock.setTime(t0 + 1_600);
        IngestResult expired = service.ingest(s(30, t0 + 50, "w0-toolate"));
        log.info("[迟到宽限] now={}, 超宽限样本 -> reason={}", clock.millis(),
                expired.rejectReason());
        assertEquals(RejectReason.TOO_OLD, expired.rejectReason());
        WindowAggregation w0After =
                service.query("temp", Map.of("sensor", "s1"), t0, t0).get(0);
        assertEquals(2, w0After.count());
        assertEquals(30.0, w0After.sum(), 1e-9);
    }

    @Test
    void samplesOnWindowBoundaryBelongToExactlyOneWindow() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        MetricSample atBoundary = s(7, t0 + 1_000, "boundary"); // 恰好边界点
        IngestResult r = service.ingest(atBoundary);
        log.info("[边界] 边界样本 ts={} -> 归入窗口={}", atBoundary.timestamp(), r.windowStartMs());
        assertTrue(r.accepted());
        assertEquals(t0 + 1_000, r.windowStartMs(), "边界点属于右侧窗口（左闭右开）");

        List<WindowAggregation> all =
                service.query("temp", Map.of("sensor", "s1"), t0, t0 + 2_000)
                        .stream().sorted(Comparator.comparingLong(WindowAggregation::windowStart))
                        .toList();
        long total = all.stream().mapToLong(WindowAggregation::count).sum();
        log.info("[边界] 跨边界查询命中窗口数={}, 样本总数={} (必须为1，无重复计入)",
                all.size(), total);
        assertEquals(1, total, "跨窗口边界样本不得被重复计入");
    }
}
