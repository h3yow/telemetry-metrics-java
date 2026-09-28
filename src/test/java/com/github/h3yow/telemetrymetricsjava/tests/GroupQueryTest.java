package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitException;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitReason;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按标签分组的跨序列汇总测试：
 * 1) 指定 groupBy 后同组多序列正确合并：样本数/序列数/sum/min/max/avg/按事件时间最后值；
 * 2) 未参与分组的标签不会拆分组；groupBy 为空是整指标一个全局组；缺失分组键取空串；
 * 3) 结果按窗口、分组稳定排序，重复运行一致；空分组不凭空出现；
 * 4) 四类容量越界分别抛出可区分异常，不静默截断/合并。
 */
class GroupQueryTest {

    private static final Logger log = LoggerFactory.getLogger(GroupQueryTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(5);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(60_000);
        return new MetricsService(p, clock);
    }

    private static MetricSample s(double value, long ts, Map<String, String> labels, String id) {
        return new MetricSample("cpu", value, ts, labels, id);
    }

    @Test
    void mergesSeriesByHostAcrossIgnoredLabels() {
        long t0 = 10_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        // host=a 两台不同 service 的机器；host=b 一台；service 不在 groupBy -> 不拆分组
        List<MetricSample> input = List.of(
                s(10, t0 + 100, Map.of("host", "a", "service", "web"), "a1"),
                s(20, t0 + 200, Map.of("host", "a", "service", "db"), "a2"),
                s(30, t0 + 300, Map.of("host", "b", "service", "web"), "b1"));
        for (MetricSample in : input) {
            assertTrue(service.ingest(in).accepted());
        }
        log.info("[分组查询] 输入: {}", input.stream().map(x -> x.value() + "" + x.labels()).toList());

        List<GroupWindowAggregation> rows =
                service.queryGrouped("cpu", List.of("host"), t0, t0);
        log.info("[分组查询] groupBy=host -> {}", rows.stream()
                .map(g -> g.groupLabels() + "(samples=" + g.sampleCount() + ",series="
                        + g.seriesCount() + ",sum=" + g.sum() + ",min=" + g.min() + ",max="
                        + g.max() + ",avg=" + g.average() + ",last=" + g.lastValue() + ")").toList());

        assertEquals(2, rows.size(), "只应有 host=a / host=b 两组，service 不拆分组");
        GroupWindowAggregation a = rows.get(0);
        assertEquals(Map.of("host", "a"), a.groupLabels());
        assertEquals(2, a.sampleCount());
        assertEquals(2, a.seriesCount());
        assertEquals(30.0, a.sum(), 1e-9);
        assertEquals(10.0, a.min(), 1e-9);
        assertEquals(20.0, a.max(), 1e-9);
        assertEquals(15.0, a.average(), 1e-9);
        assertEquals(20.0, a.lastValue(), 1e-9, "按事件时间的最后值应为 t0+200 的 20");

        GroupWindowAggregation b = rows.get(1);
        assertEquals(Map.of("host", "b"), b.groupLabels());
        assertEquals(1, b.seriesCount());
        assertEquals(30.0, b.lastValue(), 1e-9);
    }

    @Test
    void emptyGroupByIsSingleGlobalGroupAndMissingKeyIsEmptyString() {
        long t0 = 20_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        assertTrue(service.ingest(s(5, t0 + 10, Map.of("host", "a"), "g1")).accepted());
        assertTrue(service.ingest(s(7, t0 + 20, Map.of("host", "b"), "g2")).accepted());
        // 一条完全没有 host 标签的序列：按 host 分组时缺失键取空串，单独成组
        assertTrue(service.ingest(s(9, t0 + 30, Map.of("other", "x"), "g3")).accepted());

        List<GroupWindowAggregation> global = service.queryGrouped("cpu", List.of(), t0, t0);
        assertEquals(1, global.size(), "groupBy 为空时整指标一个全局组");
        assertEquals(Map.of(), global.get(0).groupLabels());
        assertEquals(3, global.get(0).seriesCount());
        assertEquals(21.0, global.get(0).sum(), 1e-9);

        List<GroupWindowAggregation> byHost =
                service.queryGrouped("cpu", List.of("host"), t0, t0);
        assertEquals(3, byHost.size());
        assertEquals(Map.of("host", ""), byHost.get(0).groupLabels(), "缺失分组键取空串且排序最前");
        assertEquals(Map.of("host", "a"), byHost.get(1).groupLabels());
        assertEquals(Map.of("host", "b"), byHost.get(2).groupLabels());
        log.info("[分组查询] 缺失键分组: {}", byHost.stream().map(GroupWindowAggregation::groupLabels).toList());
    }

    @Test
    void outputIsStableAcrossRepeatedQueriesAndWindowsSorted() {
        long t0 = 30_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService service = newService(clock);

        for (int w = 0; w < 3; w++) {
            long start = t0 + w * 1_000L;
            assertTrue(service.ingest(s(w + 1, start + 10, Map.of("host", "b"), "b" + w)).accepted());
            assertTrue(service.ingest(s(w + 10, start + 20, Map.of("host", "a"), "a" + w)).accepted());
        }
        List<String> first = render(service.queryGrouped("cpu", List.of("host"), t0, t0 + 2_000));
        List<String> second = render(service.queryGrouped("cpu", List.of("host"), t0, t0 + 2_000));
        log.info("[稳定性] 分组查询结果: {}", first);
        assertEquals(first, second, "相同输入重复查询结果必须一致");
        assertEquals(6, first.size(), "3 窗口 × 2 分组");
        // 先按窗口起点、再按分组标签排序
        assertEquals("[w0 host=a]", first.get(0));
        assertEquals("[w0 host=b]", first.get(1));
        assertEquals("[w2 host=b]", first.get(5));

        // 查一个无任何样本的时间区间：不凭空产生空分组
        assertTrue(service.queryGrouped("cpu", List.of("host"), t0 + 9_000, t0 + 9_000).isEmpty());
        // 查一个不存在的指标：同样为空
        assertTrue(service.queryGrouped("nope", List.of("host"), t0, t0).isEmpty());
    }

    @Test
    void distinctLimitsRaiseDistinguishableExceptions() {
        long t0 = 40_000;
        MutableClock clock = new MutableClock(t0);
        MetricsProperties props = new MetricsProperties();
        props.setWindowSizeMs(1_000);
        props.setRetainedWindows(5);
        props.setLateArrivalGraceMs(0);
        props.setMaxFutureSkewMs(60_000);
        props.setMaxGroupCombinations(2);
        props.setMaxSeriesPerGroup(2);
        props.setMaxGroupedQueryRows(10);
        props.setMaxQueryWindows(10);
        MetricsService service = new MetricsService(props, clock);

        // 3 个不同 host -> 超过 maxGroupCombinations=2
        service.ingest(s(1, t0, Map.of("host", "a"), "c1"));
        service.ingest(s(1, t0, Map.of("host", "b"), "c2"));
        service.ingest(s(1, t0, Map.of("host", "c"), "c3"));
        GroupLimitException comb = assertThrows(GroupLimitException.class,
                () -> service.queryGrouped("cpu", List.of("host"), t0, t0));
        assertEquals(GroupLimitReason.TOO_MANY_GROUP_COMBINATIONS, comb.reason());
        log.info("[容量] 组合数越界 reason={} limit={} actual={}", comb.reason(), comb.limit(), comb.actual());

        // 独立实例：同 host 下 3 条不同序列 -> 超过 maxSeriesPerGroup=2（组合数足够）
        MetricsService perGroupService = new MetricsService(props, new MutableClock(t0));
        perGroupService.ingest(s(1, t0, Map.of("host", "g", "service", "1"), "p1"));
        perGroupService.ingest(s(1, t0, Map.of("host", "g", "service", "2"), "p2"));
        perGroupService.ingest(s(1, t0, Map.of("host", "g", "service", "3"), "p3"));
        GroupLimitException perGroup = assertThrows(GroupLimitException.class,
                () -> perGroupService.queryGrouped("cpu", List.of("host"), t0, t0));
        assertEquals(GroupLimitReason.TOO_MANY_SERIES_IN_GROUP, perGroup.reason());
        log.info("[容量] 组内序列越界 reason={}", perGroup.reason());

        // 窗口数越界：11 个窗口 > maxQueryWindows=10（与数据无关，纯范围校验）
        GroupLimitException windows = assertThrows(GroupLimitException.class,
                () -> service.queryGrouped("cpu", List.of("host"), t0, t0 + 11_000));
        assertEquals(GroupLimitReason.TOO_MANY_QUERY_WINDOWS, windows.reason());
        log.info("[容量] 窗口数越界 reason={}", windows.reason());
    }

    @Test
    void resultRowsLimitRaisedWhenRowsExceedCap() {
        long t0 = 60_000;
        MutableClock clock = new MutableClock(t0);
        MetricsProperties props = new MetricsProperties();
        props.setWindowSizeMs(1_000);
        props.setRetainedWindows(10);
        props.setLateArrivalGraceMs(0);
        props.setMaxFutureSkewMs(60_000);
        props.setMaxGroupCombinations(100);
        props.setMaxSeriesPerGroup(100);
        props.setMaxGroupedQueryRows(4); // 2 窗口 × 3 分组 = 6 > 4
        props.setMaxQueryWindows(100);
        MetricsService service = new MetricsService(props, clock);
        for (int w = 0; w < 2; w++) {
            long start = t0 + w * 1_000L;
            for (String h : new String[]{"a", "b", "c"}) {
                service.ingest(s(1, start + 1, Map.of("host", h), h + w));
            }
        }
        GroupLimitException ex = assertThrows(GroupLimitException.class,
                () -> service.queryGrouped("cpu", List.of("host"), t0, t0 + 1_000));
        assertEquals(GroupLimitReason.TOO_MANY_RESULT_ROWS, ex.reason());
        log.info("[容量] 返回行数越界 reason={} actual={}", ex.reason(), ex.actual());
    }

    private static List<String> render(List<GroupWindowAggregation> rows) {
        return rows.stream()
                .map(g -> "[w" + (g.windowStart() - 30_000) / 1_000 + " "
                        + GroupQueryTest.renderLabels(g.groupLabels()) + "]")
                .toList();
    }

    private static String renderLabels(Map<String, String> labels) {
        StringBuilder sb = new StringBuilder();
        labels.forEach((k, v) -> sb.append(k).append('=').append(v));
        return sb.toString();
    }
}
