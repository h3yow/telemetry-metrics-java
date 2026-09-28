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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分组告警测试：
 * 1) 每个分组独立滞回：一组越线触发不影响另一组；事件/状态携带分组身份与聚合依据；
 * 2) 分组中途消失（缺窗口）不能被当成 0 值或正常值推进状态；保留期内回来不算新触发，
 *    状态未变不反复告警；
 * 3) 恢复后再次越线必须重新触发；
 * 4) 超过保留期后回来按新组处理（重新触发），且缺窗口不补发 RESOLVED；
 * 5) 同一分组同一窗口（即使组内多条序列）只产生一次判定/事件。
 */
class GroupAlertEvaluationTest {

    private static final Logger log = LoggerFactory.getLogger(GroupAlertEvaluationTest.class);

    private static MetricsService newService(MutableClock clock, int retentionGroups) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(40);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(120_000);
        p.setGroupAlertRetentionWindows(retentionGroups);
        return new MetricsService(p, clock);
    }

    /** 在窗口 w 为指定 host 写入样本，推进时钟并封定，返回翻转事件。 */
    private static List<AlertEvent> pushGroupWindow(MetricsService service, MutableClock clock,
                                                    long t0, int w,
                                                    List<Map.Entry<String, Double>> hostValues) {
        long start = t0 + w * 1_000L;
        for (Map.Entry<String, Double> hv : hostValues) {
            var in = service.ingest(new MetricSample("cpu", hv.getValue(), start + 100,
                    Map.of("host", hv.getKey()), "g-" + w + "-" + hv.getKey()));
            assertTrue(in.accepted(), () -> in.detail());
        }
        clock.setTime(start + 1_000); // grace=0：进入下一窗口即封定当前窗口
        service.maintain();
        List<AlertEvent> events = service.drainAlertEvents();
        log.info("[分组告警] w{} 输入 {} -> 翻转事件 {}", w, hostValues,
                events.stream().map(e -> e.groupLabels() + "=" + e.state()
                        + "(observed=" + e.observed() + ",series=" + e.seriesCount()
                        + ",samples=" + e.sampleCount() + ")").toList());
        return events;
    }

    private static Map.Entry<String, Double> hv(String host, double v) {
        return Map.entry(host, v);
    }

    @Test
    void groupsAreEvaluatedIndependentlyAndEventsCarryGroupBasis() {
        long t0 = 100_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock, 6);
        s.registerRule(new AlertRule("grp-cpu", "cpu", Map.of(),
                AlertRule.Aggregator.MAX, 80.0, 50.0, List.of("host")));
        log.info("[分组告警] 注册按 host 分组规则: fire>=80 recover<=50, aggregator=MAX");

        // w0：a 越线(90)、b 正常(40)。只有 a 触发。
        List<AlertEvent> e0 = pushGroupWindow(s, clock, t0, 0, List.of(hv("a", 90), hv("b", 40)));
        assertEquals(1, e0.size());
        assertEquals("a", e0.get(0).groupLabels().get("host"));
        assertTrue(e0.get(0).grouped());
        assertEquals(AlertState.FIRING, e0.get(0).state());
        assertEquals(1, e0.get(0).seriesCount());
        assertEquals(1, e0.get(0).sampleCount());
        assertTrue(e0.get(0).reason().contains("group={host=a}"),
                "判定依据必须能看到分组与观测值: " + e0.get(0).reason());

        // w1：a 仍越线(85) 不重复告警；b 越线(95) 触发。
        List<AlertEvent> e1 = pushGroupWindow(s, clock, t0, 1, List.of(hv("a", 85), hv("b", 95)));
        assertEquals(1, e1.size());
        assertEquals("b", e1.get(0).groupLabels().get("host"));

        // w2：host=a 两台 machine（额外标签不拆组）组内 MAX=92；b=60 落在死区维持 FIRING。无翻转。
        long start = t0 + 2_000L;
        s.ingest(new MetricSample("cpu", 60.0, start + 100,
                Map.of("host", "a", "machine", "m1"), "x1"));
        s.ingest(new MetricSample("cpu", 92.0, start + 200,
                Map.of("host", "a", "machine", "m2"), "x2"));
        s.ingest(new MetricSample("cpu", 60.0, start + 100,
                Map.of("host", "b", "machine", "m3"), "x3"));
        clock.setTime(start + 1_000);
        s.maintain();
        List<AlertEvent> e2 = s.drainAlertEvents();
        log.info("[分组告警] w2 多序列同组事件: {}",
                e2.stream().map(e -> e.groupLabels() + "=" + e.state()).toList());
        assertTrue(e2.isEmpty(), "a、b 均已 FIRING，状态未变不应重复事件");

        AlertStatusView aStatus = s.groupAlertStatuses().stream()
                .filter(v -> v.groupLabels().get("host").equals("a")).findFirst().orElseThrow();
        assertEquals(AlertState.FIRING, aStatus.state());
        assertEquals(92.0, aStatus.lastObservedValue(), 1e-9,
                "组内 MAX 必须跨序列取最大值作为判定依据");
        assertTrue(aStatus.grouped());
    }

    @Test
    void missingWindowDoesNotResetAndReappearWithinRetentionDoesNotRefire() {
        long t0 = 200_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock, 5);
        s.registerRule(new AlertRule("grp", "cpu", Map.of(),
                AlertRule.Aggregator.LAST, 80.0, 50.0, List.of("host")));

        // w0: a=90 -> FIRING
        assertEquals(AlertState.FIRING,
                pushGroupWindow(s, clock, t0, 0, List.of(hv("a", 90))).get(0).state());

        // w1、w2：host=a 完全没有样本（分组消失），用 other 维持窗口封定推进。
        // 绝不能把缺窗口当成 0/正常值 -> 不得 RESOLVE。
        for (int w = 1; w <= 2; w++) {
            List<AlertEvent> ev = pushGroupWindow(s, clock, t0, w, List.of(hv("other", 1)));
            assertEquals(List.of(), ev, "w" + w + " 缺 a 组样本不得推进 a 的状态");
        }
        AlertStatusView a = s.groupAlertStatuses().stream()
                .filter(v -> v.groupLabels().get("host").equals("a")).findFirst().orElseThrow();
        assertEquals(AlertState.FIRING, a.state(), "缺窗口不得把 a 当成恢复");
        assertEquals(t0, a.lastEvaluatedWindowStart(), "最后判定窗口仍停留在 w0");

        // w3：a 在保留期内回来且仍越线(95)。状态没变 -> 不得产生新 FIRING 事件。
        List<AlertEvent> e3 = pushGroupWindow(s, clock, t0, 3, List.of(hv("a", 95)));
        assertEquals(List.of(), e3, "保留期内回来且仍 FIRING，不算新触发，不重复告警");

        // w4：a 降到 40 -> 真正恢复（需要真实低观测值，而非缺窗口）。
        assertEquals(AlertState.RESOLVED,
                pushGroupWindow(s, clock, t0, 4, List.of(hv("a", 40))).get(0).state());

        // w5：a 再次越线 90 -> 必须重新触发。
        assertEquals(AlertState.FIRING,
                pushGroupWindow(s, clock, t0, 5, List.of(hv("a", 90))).get(0).state());
    }

    @Test
    void reappearAfterRetentionIsTreatedAsFreshGroupWithoutSyntheticResolve() {
        long t0 = 300_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock, 2); // 保留 2 个窗口
        s.registerRule(new AlertRule("grp2", "cpu", Map.of(),
                AlertRule.Aggregator.LAST, 80.0, 50.0, List.of("host")));

        // w0: a=90 FIRING
        assertEquals(1, pushGroupWindow(s, clock, t0, 0, List.of(hv("a", 90))).size());

        // w1..w4：a 连续缺席超过保留期（用 other 维持窗口封定）。期间不得补发 RESOLVED。
        List<AlertEvent> duringAbsence = new ArrayList<>();
        for (int w = 1; w <= 4; w++) {
            duringAbsence.addAll(pushGroupWindow(s, clock, t0, w, List.of(hv("other", 1))));
        }
        assertTrue(duringAbsence.isEmpty(), "分组消失超期不得补发合成 RESOLVED 事件");
        assertFalse(s.groupAlertStatuses().stream()
                        .anyMatch(v -> v.groupLabels().get("host").equals("a")),
                "超过保留期后分组状态条目应被淘汰，状态表不无限增长");

        // w5：a 回来且越线 -> 作为全新 RESOLVED 组重新触发一次 FIRING。
        List<AlertEvent> again = pushGroupWindow(s, clock, t0, 5, List.of(hv("a", 90)));
        assertEquals(1, again.size());
        assertEquals(AlertState.FIRING, again.get(0).state());
        assertEquals(t0 + 5_000L, again.get(0).windowStart());
    }

    @Test
    void oneDecisionPerGroupPerWindowEvenWithMultipleSeries() {
        long t0 = 400_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock, 5);
        s.registerRule(new AlertRule("grp3", "cpu", Map.of(),
                AlertRule.Aggregator.AVG, 80.0, 50.0, List.of("host")));

        // 同窗口 host=a 有 3 条序列都越线：整组只能判定一次、产生一个事件。
        long start = t0;
        for (int m = 0; m < 3; m++) {
            var in = s.ingest(new MetricSample("cpu", 90.0, start + 100 + m,
                    Map.of("host", "a", "machine", "m" + m), "p" + m));
            assertTrue(in.accepted(), in.detail());
        }
        clock.setTime(start + 1_000);
        s.maintain();
        List<AlertEvent> events = s.drainAlertEvents();
        log.info("[分组告警] 同窗口 3 序列事件数={} (必须为1)", events.size());
        assertEquals(1, events.size(), "同一分组同一窗口只能产生一次判定/事件");
        assertEquals(3, events.get(0).seriesCount());
        assertEquals(3, events.get(0).sampleCount());
        assertEquals(90.0, events.get(0).observed(), 1e-9);

        // 再次维护（重复 tick）不得重复判定。
        clock.setTime(start + 1_500);
        s.maintain();
        assertTrue(s.drainAlertEvents().isEmpty(), "封定后的重复 tick 不得重复判定");
    }
}
