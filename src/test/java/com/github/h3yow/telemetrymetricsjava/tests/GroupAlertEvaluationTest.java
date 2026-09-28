package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertStatusView;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.FIRING;
import static com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.RESOLVED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分组告警测试：
 * 1) 组整体水位越线触发 FIRING（多序列聚合，而非单机告警）；状态不变不反复告警；
 * 2) 分组中途消失再回来：保留期内不算新触发；超过保留期后回来按新分组重新从 RESOLVED 起；
 * 3) 没有样本的窗口不被当 0 值推进（FIRING 中缺窗口不会被“恢复”）；
 * 4) 恢复后再次越线要重新触发；事件/状态可见分组、聚合与判定依据；
 * 5) 容量越界给出可区分反馈（rejectedEvaluationCount），且不影响其它规则。
 */
class GroupAlertEvaluationTest {

    private static final Logger log = LoggerFactory.getLogger(GroupAlertEvaluationTest.class);

    private static MetricsService newService(MutableClock clock, int retentionWindows) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(8);
        p.setLateArrivalGraceMs(0); // 窗口结束即封定，判定时点确定
        p.setMaxFutureSkewMs(60_000);
        p.setGroupAlertRetentionWindows(retentionWindows);
        return new MetricsService(p, clock);
    }

    /** 在窗口 w 上报若干条同 host（可能不同 service）样本，并推进到该窗口封定。 */
    private static List<GroupAlertEvent> push(MetricsService service, MutableClock clock,
                                              long t0, int w,
                                              List<Map<String, String>> labels,
                                              List<Double> values) {
        long start = t0 + w * 1_000L;
        for (int i = 0; i < labels.size(); i++) {
            assertTrue(service.ingest(new MetricSample("cpu", values.get(i),
                    start + 100 + i, labels.get(i), "w" + w + "-" + i)).accepted());
        }
        clock.setTime(start + 1_000);
        service.maintain();
        List<GroupAlertEvent> events = service.drainGroupAlertEvents();
        log.info("[分组告警] w{} values={} -> {}", w, values,
                events.stream().map(e -> e.groupKey() + ":" + e.state() + "(" + e.reason() + ")")
                        .toList());
        return events;
    }

    /** 推进到窗口 w 封定但不给该 host 任何样本（其它指标可有可无），用于模拟缺窗口。 */
    private static List<GroupAlertEvent> advanceEmpty(MetricsService service, MutableClock clock,
                                                      long t0, int w) {
        long start = t0 + w * 1_000L;
        clock.setTime(start + 1_000);
        service.maintain();
        List<GroupAlertEvent> events = service.drainGroupAlertEvents();
        log.info("[分组告警] w{} 该分组无样本 -> 翻转事件 {}", w,
                events.stream().map(e -> e.groupKey() + ":" + e.state()).toList());
        return events;
    }

    @Test
    void groupWaterLevelFiresOnceAndRecoversThenRearms() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 3);
        // 按 host 分组，用组平均值判定：两台机器平均 >=80 触发，<=50 恢复
        service.registerGroupRule(new GroupAlertRule("grp-cpu", "cpu", List.of("host"),
                AlertRule.Aggregator.AVG, 80.0, 50.0));

        // w0: 70/80 -> avg 75 正常
        assertEquals(0, push(service, clock, t0, 0,
                List.of(Map.of("host", "a", "service", "web"), Map.of("host", "a", "service", "db")),
                List.of(70.0, 80.0)).size());

        // w1: 90/90 -> avg 90 触发一次
        List<GroupAlertEvent> fire = push(service, clock, t0, 1,
                List.of(Map.of("host", "a", "service", "web"), Map.of("host", "a", "service", "db")),
                List.of(90.0, 90.0));
        assertEquals(1, fire.size());
        assertEquals(FIRING, fire.get(0).state());
        assertEquals(Map.of("host", "a"), fire.get(0).groupLabels());
        assertEquals(2, fire.get(0).aggregation().seriesCount());
        log.info("[分组告警] 触发依据: observed={} 聚合={}", fire.get(0).observed(),
                fire.get(0).aggregation());

        // w2: 95/85 -> 仍 FIRING，状态不变不重复告警
        assertEquals(0, push(service, clock, t0, 2,
                List.of(Map.of("host", "a", "service", "web"), Map.of("host", "a", "service", "db")),
                List.of(95.0, 85.0)).size());

        // w3: 40/50 -> avg 45 恢复一次
        List<GroupAlertEvent> recover = push(service, clock, t0, 3,
                List.of(Map.of("host", "a", "service", "web"), Map.of("host", "a", "service", "db")),
                List.of(40.0, 50.0));
        assertEquals(1, recover.size());
        assertEquals(RESOLVED, recover.get(0).state());

        // w4: 90/100 -> 恢复后再次越线，必须重新触发
        List<GroupAlertEvent> refire = push(service, clock, t0, 4,
                List.of(Map.of("host", "a", "service", "web"), Map.of("host", "a", "service", "db")),
                List.of(90.0, 100.0));
        assertEquals(1, refire.size());
        assertEquals(FIRING, refire.get(0).state());
    }

    @Test
    void emptyWindowsDoNotForceResolveAndReturnWithinRetentionKeepsFiring() {
        long t0 = 20_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 3);
        service.registerGroupRule(new GroupAlertRule("grp2", "cpu", List.of("host"),
                AlertRule.Aggregator.MAX, 80.0, 20.0));

        // w0 触发
        assertEquals(FIRING, push(service, clock, t0, 0,
                List.of(Map.of("host", "a")), List.of(90.0)).get(0).state());

        // w1、w2 该分组完全没有样本：绝不能被当 0 值“恢复”，也无新事件
        assertEquals(0, advanceEmpty(service, clock, t0, 1).size());
        assertEquals(0, advanceEmpty(service, clock, t0, 2).size());

        GroupAlertStatusView status = service.groupAlertStatuses().stream()
                .filter(v -> v.groupKey().equals("host=a")).findFirst().orElseThrow();
        assertEquals(FIRING, status.state(), "缺样本窗口不得推进状态");
        log.info("[分组告警] 缺窗口后状态仍为 {} lastWindow={}", status.state(),
                status.lastEvaluatedWindowStart());

        // w3 在保留期内回来且仍越线：沿用 FIRING，不算新触发（无事件）
        assertEquals(0, push(service, clock, t0, 3,
                List.of(Map.of("host", "a")), List.of(85.0)).size());
        status = service.groupAlertStatuses().stream()
                .filter(v -> v.groupKey().equals("host=a")).findFirst().orElseThrow();
        assertEquals(FIRING, status.state());

        // w4 低于恢复阈值，才真正恢复
        assertEquals(RESOLVED, push(service, clock, t0, 4,
                List.of(Map.of("host", "a")), List.of(10.0)).get(0).state());
    }

    @Test
    void groupGoneBeyondRetentionComesBackAsNewGroup() {
        long t0 = 30_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 0); // 缺席一个窗口即淘汰状态
        service.registerGroupRule(new GroupAlertRule("grp3", "cpu", List.of("host"),
                AlertRule.Aggregator.LAST, 80.0, 50.0));

        assertEquals(FIRING, push(service, clock, t0, 0,
                List.of(Map.of("host", "a")), List.of(90.0)).get(0).state());

        // w1 缺席：保留期为 0，状态立即回收（不产生任何恢复事件）
        advanceEmpty(service, clock, t0, 1);
        assertTrue(service.groupAlertStatuses().stream().noneMatch(v -> v.groupKey().equals("host=a")),
                "超过保留期的分组状态应被回收，状态条目数有界");

        // w3 回来但值在死区(50,80)：按新分组从 RESOLVED 起，不触发
        assertEquals(0, push(service, clock, t0, 3,
                List.of(Map.of("host", "a")), List.of(60.0)).size());

        // w4 再次越线 -> 作为新触发产生 FIRING
        List<GroupAlertEvent> again = push(service, clock, t0, 4,
                List.of(Map.of("host", "a")), List.of(90.0));
        assertEquals(1, again.size());
        assertEquals(FIRING, again.get(0).state());
    }

    @Test
    void groupsAreEvaluatedIndependentlyAndOncePerWindow() {
        long t0 = 40_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock, 3);
        service.registerGroupRule(new GroupAlertRule("grp4", "cpu", List.of("host"),
                AlertRule.Aggregator.LAST, 80.0, 80.0));

        // 同一窗口两个 host：a 越线、b 不越线，各自独立
        List<GroupAlertEvent> ev = push(service, clock, t0, 0,
                List.of(Map.of("host", "a"), Map.of("host", "b")),
                List.of(90.0, 10.0));
        assertEquals(1, ev.size());
        assertEquals("host=a", ev.get(0).groupKey());

        // 再维护一次（同窗口无新封定）：不得重复判定、重复告警
        service.maintain();
        assertTrue(service.drainGroupAlertEvents().isEmpty());

        // 两个分组的状态都可查询
        Map<String, GroupAlertStatusView> statuses = new java.util.HashMap<>();
        for (GroupAlertStatusView v : service.groupAlertStatuses()) {
            statuses.put(v.groupKey(), v);
        }
        assertEquals(FIRING, statuses.get("host=a").state());
        assertEquals(RESOLVED, statuses.get("host=b").state());
    }

    @Test
    void groupCapacityBreachIsReportedAndSkipsOnlyThatRuleWindow() {
        long t0 = 50_000;
        MutableClock clock = new MutableClock(t0);
        MetricsProperties props = new MetricsProperties();
        props.setWindowSizeMs(1_000);
        props.setRetainedWindows(5);
        props.setLateArrivalGraceMs(0);
        props.setMaxFutureSkewMs(60_000);
        props.setMaxGroupCombinations(2); // 3 个 host 会越界
        props.setMaxSeriesPerGroup(100);
        props.setGroupAlertRetentionWindows(3);
        MetricsService service = new MetricsService(props, clock);

        // 受限规则按 host 分组（会因 3 个组合越界）；对照规则用空 groupBy（单个全局组，不受影响）
        service.registerGroupRule(new GroupAlertRule("limited", "cpu", List.of("host"),
                AlertRule.Aggregator.LAST, 80.0, 50.0));
        service.registerGroupRule(new GroupAlertRule("global", "cpu", List.of(),
                AlertRule.Aggregator.MAX, 80.0, 50.0));

        for (String h : new String[]{"a", "b", "c"}) {
            assertTrue(service.ingest(new MetricSample("cpu", 90.0, t0 + 100,
                    Map.of("host", h), "s-" + h)).accepted());
        }
        clock.setTime(t0 + 1_000);
        service.maintain();

        assertEquals(1L, service.groupAlertRejectedCount(), "受限规则本窗评估应被跳过并计数 1 次");
        Map<String, String> rej = service.groupAlertLastRejections();
        assertTrue(rej.containsKey("limited#" + t0), "越界信息应带规则与窗口: " + rej.keySet());
        assertTrue(rej.get("limited#" + t0).contains("TOO_MANY_GROUP_COMBINATIONS"),
                "越界原因必须可区分: " + rej.get("limited#" + t0));
        log.info("[分组告警-容量] rejectedCount={} detail={}",
                service.groupAlertRejectedCount(), rej);

        // 只取一次事件：受限规则不得凭空告警，对照规则（全局组 max=90）应正常触发
        List<com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent> ev =
                service.drainGroupAlertEvents();
        assertTrue(ev.stream().noneMatch(e -> e.ruleId().equals("limited")),
                "容量越界时不能产生错误的分组告警");
        assertTrue(ev.stream().anyMatch(e -> e.ruleId().equals("global")),
                "越界只影响受限规则，其它规则照常判定");
    }
}
