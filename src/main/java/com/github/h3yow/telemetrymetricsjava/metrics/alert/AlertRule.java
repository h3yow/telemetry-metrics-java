package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.List;
import java.util.Map;

/**
 * 告警规则：对某个指标 + 标签匹配的序列，在每个窗口封定时判定。
 * 阈值带滞回（hysteresis）：观测值 &gt;= fireThreshold 进入 FIRING，
 * 观测值 &lt;= recoverThreshold 才回到 RESOLVED，两阈值之间的区间为“维持现状”死区，
 * 阈值附近抖动不会反复触发。
 *
 * <p><b>分组规则</b>：{@code groupByKeys} 非空时，规则不再对每条序列独立判定，
 * 而是把同一窗口内、标签匹配的序列按这些标签键投影分组，对每个分组的
 * {@link GroupWindowAggregation} 独立做滞回判定；空列表表示单序列模式（兼容旧行为）。</p>
 *
 * @param ruleId           规则唯一 ID
 * @param metricName       目标指标名
 * @param labelMatchers    标签匹配（精确相等；空 map 表示该指标下所有序列）
 * @param aggregator       判定所用聚合字段
 * @param fireThreshold    触发阈值（&gt;= 进入 FIRING）
 * @param recoverThreshold 恢复阈值（&lt;= 回到 RESOLVED；必须 &lt;= fireThreshold）
 * @param groupByKeys      分组标签键；空列表 = 单序列模式
 */
public record AlertRule(String ruleId, String metricName,
                        Map<String, String> labelMatchers,
                        Aggregator aggregator,
                        double fireThreshold, double recoverThreshold,
                        List<String> groupByKeys) {

    /** 规则判定使用的聚合字段。 */
    public enum Aggregator { AVG, SUM, MAX, MIN, COUNT, LAST }

    /** 单序列模式的兼容构造器（无分组）。 */
    public AlertRule(String ruleId, String metricName,
                     Map<String, String> labelMatchers,
                     Aggregator aggregator,
                     double fireThreshold, double recoverThreshold) {
        this(ruleId, metricName, labelMatchers, aggregator, fireThreshold, recoverThreshold,
                List.of());
    }

    public AlertRule {
        labelMatchers = labelMatchers == null ? Map.of() : Map.copyOf(labelMatchers);
        groupByKeys = groupByKeys == null ? List.of() : List.copyOf(groupByKeys);
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must be present");
        }
        if (metricName == null || metricName.isBlank()) {
            throw new IllegalArgumentException("metricName must be present");
        }
        if (aggregator == null) {
            throw new IllegalArgumentException("aggregator must be present");
        }
        if (recoverThreshold > fireThreshold) {
            throw new IllegalArgumentException(
                    "recoverThreshold must be <= fireThreshold for hysteresis");
        }
    }

    /** 是否为分组规则。 */
    public boolean grouped() {
        return !groupByKeys.isEmpty();
    }

    /** 指标名相等且规则声明的每个标签键值对都在样本标签中存在（额外标签不影响匹配）。 */
    boolean matches(String metricName, Map<String, String> labels) {
        if (!this.metricName.equals(metricName)) {
            return false;
        }
        for (Map.Entry<String, String> e : labelMatchers.entrySet()) {
            if (!e.getValue().equals(labels.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /** 按规则配置的聚合字段从窗口快照中取判定值。 */
    double extract(WindowAggregation agg) {
        return switch (aggregator) {
            case AVG -> agg.average();
            case SUM -> agg.sum();
            case MAX -> agg.max();
            case MIN -> agg.min();
            case COUNT -> agg.count();
            case LAST -> agg.lastValue();
        };
    }

    /** 按规则配置的聚合字段从分组窗口快照中取判定值（COUNT 取组内样本总数）。 */
    double extractGroup(GroupWindowAggregation agg) {
        return switch (aggregator) {
            case AVG -> agg.average();
            case SUM -> agg.sum();
            case MAX -> agg.max();
            case MIN -> agg.min();
            case COUNT -> agg.sampleCount();
            case LAST -> agg.lastValue();
        };
    }
}
