package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import java.util.Map;

/**
 * 状态变化事件（仅在 FIRING/RESOLVED 状态真正翻转时产生）。
 *
 * <p>单序列规则的事件与旧版本完全一致：{@code grouped=false}、
 * {@code groupLabels} 为空 map。分组规则的事件额外携带：{@code grouped=true}、
 * {@code groupLabels}（触发分组的标签子集）、{@code seriesCount}/{@code sampleCount}
 * （聚合依据），此时 {@code seriesKey} 为“指标名{分组标签}”形式的分组渲染名。</p>
 *
 * @param ruleId       规则 ID
 * @param seriesKey    序列键（单序列）或分组渲染名（分组规则）
 * @param state        变化后的状态
 * @param windowStart  触发判定的窗口
 * @param observed     当时观测到的聚合值
 * @param reason       可解释的判定依据
 * @param eventTimeMs  事件产生时间
 * @param grouped      是否来自分组规则
 * @param groupLabels  分组标签（仅含分组键；单序列事件为空 map）
 * @param seriesCount  聚合覆盖的序列数（单序列事件为 1）
 * @param sampleCount  聚合覆盖的样本数
 */
public record AlertEvent(String ruleId, String seriesKey, AlertState state,
                         long windowStart, double observed, String reason, long eventTimeMs,
                         boolean grouped, Map<String, String> groupLabels,
                         long seriesCount, long sampleCount) {

    /** 单序列事件的兼容构造器。 */
    public AlertEvent(String ruleId, String seriesKey, AlertState state,
                      long windowStart, double observed, String reason, long eventTimeMs) {
        this(ruleId, seriesKey, state, windowStart, observed, reason, eventTimeMs,
                false, Map.of(), 1L, 0L);
    }
}
