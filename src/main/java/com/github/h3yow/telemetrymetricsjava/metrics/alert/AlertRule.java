package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.Map;

/**
 * 告警规则：对某个指标 + 标签匹配的序列，在每个窗口封定时判定。
 * 阈值带滞回（hysteresis）：观测值 &gt;= fireThreshold 进入 FIRING，
 * 观测值 &lt;= recoverThreshold 才回到 RESOLVED，两阈值之间的区间为“维持现状”死区，
 * 阈值附近抖动不会反复触发。
 *
 * @param ruleId           规则唯一 ID
 * @param metricName       目标指标名
 * @param labelMatchers    标签匹配（精确相等；空 map 表示该指标下所有序列）
 * @param aggregator       判定所用聚合字段
 * @param fireThreshold    触发阈值（&gt;= 进入 FIRING）
 * @param recoverThreshold 恢复阈值（&lt;= 回到 RESOLVED；必须 &lt;= fireThreshold）
 */
public record AlertRule(String ruleId, String metricName,
                        Map<String, String> labelMatchers,
                        Aggregator aggregator,
                        double fireThreshold, double recoverThreshold) {

    /** 规则判定使用的聚合字段。 */
    public enum Aggregator { AVG, SUM, MAX, MIN, COUNT, LAST }

    public AlertRule {
        labelMatchers = labelMatchers == null ? Map.of() : Map.copyOf(labelMatchers);
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
}
