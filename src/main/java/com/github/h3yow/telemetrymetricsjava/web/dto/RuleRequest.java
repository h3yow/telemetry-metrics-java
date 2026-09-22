package com.github.h3yow.telemetrymetricsjava.web.dto;

import java.util.Map;

/** 告警规则创建请求体。 */
public record RuleRequest(String ruleId, String metricName, Map<String, String> requiredTags,
                          String function, String op, Double threshold) {
}
