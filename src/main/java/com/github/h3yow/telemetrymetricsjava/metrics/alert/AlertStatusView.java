package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import java.util.Map;

/**
 * 规则在某条序列（或某个分组）上的当前状态视图（不可变快照）。
 *
 * <p>单序列状态与旧版本字段一致；分组状态额外携带 {@code grouped=true} 与
 * {@code groupLabels}。</p>
 */
public record AlertStatusView(String ruleId, String seriesKey, AlertState state,
                              long lastEvaluatedWindowStart, double lastObservedValue,
                              long stateChangedAtWindowStart,
                              boolean grouped, Map<String, String> groupLabels) {

    /** 单序列状态的兼容构造器。 */
    public AlertStatusView(String ruleId, String seriesKey, AlertState state,
                           long lastEvaluatedWindowStart, double lastObservedValue,
                           long stateChangedAtWindowStart) {
        this(ruleId, seriesKey, state, lastEvaluatedWindowStart, lastObservedValue,
                stateChangedAtWindowStart, false, Map.of());
    }
}
