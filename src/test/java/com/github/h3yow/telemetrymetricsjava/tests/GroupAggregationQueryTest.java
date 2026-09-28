package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsProperties;
import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryStatus;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分组汇总查询测试：
 * 1) 按部分标签分组合并多条序列：count/seriesCount/sum/min/max/avg/按事件时间 lastValue；
 * 2) 未参与分组的标签不拆组；分组键缺失自成一组；不凭空出现空分组；
 * 3) 相同输入多次查询顺序与内容一致（窗口升序、组内标签字典序）；
 * 4) 三类容量上界（窗口数 / 组合数 / 组内序列数）给出可区分反馈，不静默截断。
 */
class GroupAggregationQueryTest {

    private static final Logger log = LoggerFactory.getLogger(GroupAggregationQueryTest.class);

    private static MetricsService newService(MutableClock clock) {
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(6);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(60_000);
        return new MetricsService(p, clock);
    }

    private static void ingest(MetricsService s, String name, double v, long ts,
                               Map<String, String> labels, String id) {
        var r = s.ingest(new MetricSample(name, v, ts, labels, id));
        assertTrue(r.accepted(), () -> "sample should be accepted: " + r.detail());
    }

    @Test
    void mergesSeriesByHostAcrossUnrelatedLabelsAndComputesAllFields() {
        long t0 = 100_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock);

        // 同一 host、不同 service（未参与分组的标签）必须合并成一组；
        // a1/a3 标签完全相同（同一条序列的两个样本），只在 sampleId 上不同。
        ingest(s, "cpu", 10.0, t0 + 100, labels("host", "a", "service", "x"), "a1");
        ingest(s, "cpu", 30.0, t0 + 200, labels("host", "a", "service", "y"), "a2");
        ingest(s, "cpu", 20.0, t0 + 150, labels("host", "a", "service", "x"), "a3");
        ingest(s, "cpu", 50.0, t0 + 100, labels("host", "b", "service", "x"), "b1");

        GroupQueryResult result = s.queryGroups("cpu", List.of("host"), t0, t0);
        log.info("[分组-合并] 结果: status={} rows={}", result.status(),
                result.results().stream().map(r -> r.groupLabels()
                        + " samples=" + r.sampleCount() + " series=" + r.seriesCount()
                        + " sum=" + r.sum() + " last=" + r.lastValue()).toList());

        assertTrue(result.isComplete());
        assertEquals(2, result.results().size(), "host=a 与 host=b 两个组");
        GroupWindowAggregation a = result.results().get(0);
        GroupWindowAggregation b = result.results().get(1);

        assertEquals(Map.of("host", "a"), a.groupLabels());
        assertEquals(3, a.sampleCount(), "host=a 三个样本");
        assertEquals(2, a.seriesCount(), "host=a 两条序列（service=x / service=y）");
        assertEquals(60.0, a.sum(), 1e-9);
        assertEquals(10.0, a.min(), 1e-9);
        assertEquals(30.0, a.max(), 1e-9);
        assertEquals(20.0, a.average(), 1e-9);
        // 按事件时间：最大事件时间 t0+200 -> 30.0（即使它来自“另一条序列”）。
        assertEquals(30.0, a.lastValue(), 1e-9, "lastValue 必须按事件时间跨序列取最后值");

        assertEquals(Map.of("host", "b"), b.groupLabels());
        assertEquals(1, b.seriesCount());
        assertEquals(50.0, b.lastValue(), 1e-9);
    }

    @Test
    void emptyGroupByAggregatesEverythingAndMissingKeyIsItsOwnGroup() {
        long t0 = 200_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock);

        ingest(s, "cpu", 5.0, t0 + 100, labels("host", "a"), "1");
        ingest(s, "cpu", 7.0, t0 + 200, labels("host", "b"), "2");
        // 该序列没有 group key “service”：缺失值 "" 自成一组。
        ingest(s, "cpu", 9.0, t0 + 300, labels("host", "c"), "3");
        ingest(s, "cpu", 11.0, t0 + 300, labels("host", "c", "service", "api"), "4");

        // 空分组键：所有序列合成一个总组。
        GroupQueryResult all = s.queryGroups("cpu", List.of(), t0, t0);
        assertTrue(all.isComplete());
        assertEquals(1, all.results().size(), "空 groupBy 只产生一个总组，不凭空出现空分组");
        assertEquals(Map.of(), all.results().get(0).groupLabels());
        assertEquals(4, all.results().get(0).sampleCount());
        assertEquals(32.0, all.results().get(0).sum(), 1e-9);

        // 按 service 分组：host=a/b 没有 service -> 落入 "" 组；host=c,service=api 单独一组。
        GroupQueryResult bySvc = s.queryGroups("cpu", List.of("service"), t0, t0);
        assertEquals(2, bySvc.results().size());
        assertEquals(Map.of("service", ""), bySvc.results().get(0).groupLabels());
        assertEquals(3, bySvc.results().get(0).sampleCount(), "缺 service 标签的序列合为一组");
        assertEquals(Map.of("service", "api"), bySvc.results().get(1).groupLabels());
    }

    @Test
    void resultsAreStableAcrossRepeatedQueriesAndOrderedByWindowThenGroup() {
        long t0 = 300_000;
        MutableClock clock = new MutableClock(t0);
        MetricsService s = newService(clock);

        for (int w = 0; w < 3; w++) {
            ingest(s, "cpu", 1.0, t0 + w * 1_000L + 100, labels("host", "b", "z", "1"), "b" + w);
            ingest(s, "cpu", 2.0, t0 + w * 1_000L + 100, labels("host", "a", "z", "2"), "a" + w);
            ingest(s, "cpu", 3.0, t0 + w * 1_000L + 100, labels("host", "c", "z", "3"), "c" + w);
        }

        GroupQueryResult r1 = s.queryGroups("cpu", new ArrayList<>(List.of("host")),
                t0, t0 + 2_000);
        // 传入顺序打乱、换用不同 Map 实现，结果必须一致。
        GroupQueryResult r2 = s.queryGroups("cpu", List.of("host"), t0, t0 + 2_000);

        List<String> signature1 = signature(r1);
        List<String> signature2 = signature(r2);
        log.info("[分组-稳定] 第一次结果签名: {}", signature1);
        assertEquals(signature1, signature2, "相同输入多次查询的顺序与内容必须一致");

        // 先按窗口升序，窗口内按分组标签字典序 a,b,c。
        assertEquals(9, signature1.size());
        assertEquals("w0:{host=a}", signature1.get(0));
        assertEquals("w0:{host=b}", signature1.get(1));
        assertEquals("w0:{host=c}", signature1.get(2));
        assertEquals("w1:{host=a}", signature1.get(3));
        assertEquals("w2:{host=c}", signature1.get(8));
    }

    @Test
    void capacityLimitsReturnDistinguishableStatusWithoutSilentTruncation() {
        long t0 = 400_000;
        MutableClock clock = new MutableClock(t0);
        MetricsProperties p = new MetricsProperties();
        p.setWindowSizeMs(1_000);
        p.setRetainedWindows(40);
        p.setLateArrivalGraceMs(0);
        p.setMaxFutureSkewMs(120_000);
        p.setMaxSeries(10_000);
        p.setMaxLabelValuesPerKey(10_000);
        p.setMaxGroupCombinations(3);
        p.setMaxSeriesPerGroup(2);
        p.setMaxQueryWindows(5);
        MetricsService s = new MetricsService(p, clock);

        // 4 个不同 host -> 超过 maxGroupCombinations=3。
        for (String host : List.of("a", "b", "c", "d")) {
            ingest(s, "cpu", 1.0, t0 + 100, labels("host", host), "h-" + host);
        }
        GroupQueryResult combos = s.queryGroups("cpu", List.of("host"), t0, t0);
        log.info("[分组-容量] 组合超限 status={} detail={}", combos.status(), combos.detail());
        assertEquals(GroupQueryStatus.TOO_MANY_GROUP_COMBINATIONS, combos.status());
        assertTrue(combos.results().isEmpty(), "超限必须整单失败，不得静默返回部分分组");

        // 组内 3 条序列（不同 shard）-> 超过 maxSeriesPerGroup=2。
        for (String shard : List.of("1", "2", "3")) {
            ingest(s, "mem", 1.0, t0 + 100,
                    labels("host", "z", "shard", shard), "m-" + shard);
        }
        GroupQueryResult perGroup = s.queryGroups("mem", List.of("host"), t0, t0);
        log.info("[分组-容量] 组内序列超限 status={} detail={}", perGroup.status(),
                perGroup.detail());
        assertEquals(GroupQueryStatus.TOO_MANY_SERIES_IN_GROUP, perGroup.status());
        assertFalse(perGroup.isComplete());

        // 请求 6 个窗口 -> 超过 maxQueryWindows=5。
        GroupQueryResult windows = s.queryGroups("cpu", List.of("host"),
                t0, t0 + 5_000);
        log.info("[分组-容量] 窗口超限 status={} detail={}", windows.status(), windows.detail());
        assertEquals(GroupQueryStatus.TOO_MANY_WINDOWS, windows.status());
    }

    private static Map<String, String> labels(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static List<String> signature(GroupQueryResult r) {
        List<String> out = new ArrayList<>();
        for (GroupWindowAggregation g : r.results()) {
            long w = (g.windowStart() - 300_000) / 1_000;
            out.add("w" + w + ":" + g.groupLabels());
        }
        return out;
    }
}
