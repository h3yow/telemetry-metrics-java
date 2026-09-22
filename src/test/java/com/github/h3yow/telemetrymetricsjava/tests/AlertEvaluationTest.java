package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertStatusView;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 告警判定测试：
 * 1) 越过 fireThreshold 触发 FIRING，事件恰好一次；
 * 2) 滞回死区内（recover &lt; v &lt; fire）不重复触发、不提前恢复；
 * 3) 低于 recoverThreshold 才恢复；恢复后再次越线重新触发；
 * 4) 状态跨窗口保持稳定，事件与窗口一一对应、可复现。
 */
class AlertEvaluationTest {

    private static final Logger log = LoggerFactory.getLogger(AlertEvaluationTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(3);
        p.setLateArrivalGraceMs(0); // 宽限为 0：窗口结束即封定，判定时点确定
        p.setMaxFutureSkewMs(10_000);
        return new MetricsService(p, clock);
    }

    /** 在窗口 w 内写一条样本并推进到该窗口封定（grace=0，下个窗口开始即封定）。 */
    private static List<AlertEvent> pushWindow(MetricsService service, MutableClock clock,
                                               long t0, int w, double value) {
        long start = t0 + w * 1_000L;
        MetricSample sample = new MetricSample("cpu", value, start + 100,
                Map.of("host", "a"), "w" + w);
        var ingested = service.ingest(sample);
        clock.setTime(start + 1_000); // 进入下一窗口 -> 当前窗口越过封定线
        service.maintain();
        List<AlertEvent> events = service.drainAlertEvents();
        log.info("[告警] 窗口{} 输入 value={} (ingest accepted={}) -> 翻转事件 {}",
                w, value, ingested.accepted(),
                events.stream().map(e -> e.state() + "@w" + (e.windowStart() - t0) / 1000
                        + "(" + e.reason() + ")").toList());
        return events;
    }

    @Test
    void hysteresisPreventsFlappingAndRecoveryRearms() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        AlertRule rule = new AlertRule("cpu-high", "cpu", Map.of(),
                AlertRule.Aggregator.LAST, 80.0, 50.0);
        service.registerRule(rule);
        log.info("[告警] 注册规则: fire>=80, recover<=50, 死区(50,80)");

        // w0=70 正常；w1=90 触发；w2=85 仍在死区(50,80)不重复触发；
        // w3=60 死区内不恢复；w4=45 恢复；w5=90 再次越线重新触发
        List<Double> values = List.of(70.0, 90.0, 85.0, 60.0, 45.0, 90.0);
        List<AlertState> transitions = new java.util.ArrayList<>();
        for (int w = 0; w < values.size(); w++) {
            for (AlertEvent e : pushWindow(service, clock, t0, w, values.get(w))) {
                transitions.add(e.state());
            }
        }

        log.info("[告警] 全流程状态翻转序列: {}", transitions);
        assertEquals(List.of(AlertState.FIRING, AlertState.RESOLVED, AlertState.FIRING),
                transitions, "只应在 w1 触发、w4 恢复、w5 重新触发");

        AlertStatusView status = service.alertStatuses().stream()
                .filter(v -> v.ruleId().equals("cpu-high")).findFirst().orElseThrow();
        log.info("[告警] 最终状态: state={} lastObserved={} lastWindow={}",
                status.state(), status.lastObservedValue(), status.lastEvaluatedWindowStart());
        assertEquals(AlertState.FIRING, status.state());
        assertEquals(90.0, status.lastObservedValue(), 1e-9);
    }

    @Test
    void noEventWhileStateUnchangedAndExactlyOneEventPerTransition() {
        long t0 = 20_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);
        // fire=recover=80：>=80 触发，<=80 恢复（边界相等即可恢复）
        service.registerRule(new AlertRule("cpu-edge", "cpu", Map.of("host", "a"),
                AlertRule.Aggregator.AVG, 80.0, 80.0));

        // 连续两个窗口都越线：只有第一次产生 FIRING 事件
        List<AlertEvent> e1 = pushWindow(service, clock, t0, 0, 90);
        List<AlertEvent> e2 = pushWindow(service, clock, t0, 1, 95);
        log.info("[去抖] w0 事件数={}, w1 事件数={} (持续 FIRING 不得重复告警)", e1.size(), e2.size());
        assertEquals(1, e1.size());
        assertEquals(AlertState.FIRING, e1.get(0).state());
        assertEquals(0, e2.size());

        // 回到阈值以下恢复，且恢复事件也只有一次
        List<AlertEvent> e3 = pushWindow(service, clock, t0, 2, 80);
        List<AlertEvent> e4 = pushWindow(service, clock, t0, 3, 70);
        log.info("[去抖] w2(=80) 事件={}, w3(70) 事件={}", e3.stream().map(AlertEvent::state).toList(),
                e4.stream().map(AlertEvent::state).toList());
        assertEquals(1, e3.size());
        assertEquals(AlertState.RESOLVED, e3.get(0).state());
        assertEquals(0, e4.size());
        assertFalse(service.alertStatuses().isEmpty());
    }
}
