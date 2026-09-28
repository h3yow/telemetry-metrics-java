package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import java.util.Map;

/**
 * 分组告警状态翻转事件：仅在某个分组的 FIRING/RESOLVED 状态真正翻转时产生。
 *
 * @param ruleId        规则 ID
 * @param groupLabels   分组标签（规则 groupBy 键的取值组合，按 key 排序）
 * @param groupKey      分组的稳定单行文本表示
 * @param state         翻转后的状态
 * @param windowStart   触发判定的窗口
 * @param observed      判定所用的分组聚合观测值
 * @param aggregation   当时的完整分组聚合快照
 * @param reason        可解释的判定依据
 * @param eventTimeMs   事件产生时间
 */
public record GroupAlertEvent(String ruleId, Map<String, String> groupLabels, String groupKey,
                              AlertState state, long windowStart, double observed,
                              com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation aggregation,
                              String reason, long eventTimeMs) {
}
