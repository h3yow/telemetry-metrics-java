package com.github.h3yow.telemetrymetricsjava.metrics.alert;

/**
 * 状态变化事件（仅在 FIRING/RESOLVED 状态真正翻转时产生）。
 *
 * @param ruleId      规则 ID
 * @param seriesKey   序列键（指标名 + 标签）
 * @param state       变化后的状态
 * @param windowStart 触发判定的窗口
 * @param observed    当时观测到的聚合值
 * @param reason      可解释的判定依据
 * @param eventTimeMs 事件产生时间
 */
public record AlertEvent(String ruleId, String seriesKey, AlertState state,
                         long windowStart, double observed, String reason, long eventTimeMs) {
}
