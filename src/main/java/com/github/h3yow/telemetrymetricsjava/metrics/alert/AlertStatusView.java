package com.github.h3yow.telemetrymetricsjava.metrics.alert;

/**
 * 规则在某序列上的当前状态视图（不可变快照）。
 */
public record AlertStatusView(String ruleId, String seriesKey, AlertState state,
                              long lastEvaluatedWindowStart, double lastObservedValue,
                              long stateChangedAtWindowStart) {
}
