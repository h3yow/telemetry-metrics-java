package com.github.h3yow.telemetrymetricsjava.alert;

import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;

import java.util.Map;

/**
 * 告警规则：对匹配的序列，用聚合量与阈值比较。
 *
 * @param ruleId       规则 ID
 * @param metricName   匹配的指标名
 * @param requiredTags 序列必须包含的标签键值（空 map 表示匹配该指标所有序列）
 * @param function     聚合量（avg/sum/max/min/count）
 * @param op           比较方向（gt/gte/lt/lte）
 * @param threshold    阈值
 */
public record AlertRule(String ruleId, String metricName, Map<String, String> requiredTags,
                        MetricFunction function, ComparisonOp op, double threshold) {

    public AlertRule {
        if (ruleId == null || ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must not be blank");
        }
        if (metricName == null || metricName.isBlank()) {
            throw new IllegalArgumentException("metricName must not be blank");
        }
        requiredTags = requiredTags == null ? Map.of() : Map.copyOf(requiredTags);
    }

    public boolean matches(SeriesKey key) {
        if (!metricName.equals(key.name())) {
            return false;
        }
        return requiredTags.entrySet().stream()
                .allMatch(e -> e.getValue().equals(key.tags().get(e.getKey())));
    }

    public boolean breachedBy(double value) {
        return op.test(value, threshold);
    }
}
