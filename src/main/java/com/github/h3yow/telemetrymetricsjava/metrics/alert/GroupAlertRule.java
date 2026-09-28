package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;

import java.util.List;
import java.util.TreeSet;

/**
 * 分组告警规则：在某指标上按一组标签键（groupBy）把多条序列合并成组，
 * 对每个分组独立做滞回判定。
 *
 * <p>未出现在 groupBy 中的标签不参与分组，不会把一组拆成多组；
 * groupBy 为空表示把指标下所有序列聚成一个全局组。</p>
 *
 * @param ruleId           规则唯一 ID
 * @param metricName       目标指标名
 * @param groupBy          分组标签键（规范化为有序去重列表）
 * @param aggregator       判定所用的分组聚合字段
 * @param fireThreshold    触发阈值（&gt;= 进入 FIRING）
 * @param recoverThreshold 恢复阈值（&lt;= 回到 RESOLVED；必须 &lt;= fireThreshold）
 */
public record GroupAlertRule(String ruleId, String metricName, List<String> groupBy,
                             AlertRule.Aggregator aggregator,
                             double fireThreshold, double recoverThreshold) {

    public GroupAlertRule {
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must be present");
        }
        if (metricName == null || metricName.isBlank()) {
            throw new IllegalArgumentException("metricName must be present");
        }
        if (groupBy == null) {
            throw new IllegalArgumentException("groupBy must not be null");
        }
        if (aggregator == null) {
            throw new IllegalArgumentException("aggregator must be present");
        }
        if (recoverThreshold > fireThreshold) {
            throw new IllegalArgumentException(
                    "recoverThreshold must be <= fireThreshold for hysteresis");
        }
        groupBy = List.copyOf(new TreeSet<>(groupBy));
    }

    /** 从分组聚合快照中按规则配置取判定值。 */
    double extract(GroupWindowAggregation agg) {
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
