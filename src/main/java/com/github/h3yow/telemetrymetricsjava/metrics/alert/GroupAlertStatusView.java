package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import java.util.Map;

/**
 * 分组告警规则在某个分组上的当前状态视图（不可变快照）。
 *
 * @param ruleId                     规则 ID
 * @param groupLabels                分组标签取值组合（按 key 排序）
 * @param groupKey                   分组稳定单行文本
 * @param state                      当前滞回状态
 * @param lastEvaluatedWindowStart   最近一次有样本并被判定的窗口
 * @param lastObservedValue          最近一次判定观测值
 * @param stateChangedAtWindowStart  最近一次状态翻转所在窗口
 */
public record GroupAlertStatusView(String ruleId, Map<String, String> groupLabels,
                                   String groupKey, AlertState state,
                                   long lastEvaluatedWindowStart, double lastObservedValue,
                                   long stateChangedAtWindowStart) {
}
