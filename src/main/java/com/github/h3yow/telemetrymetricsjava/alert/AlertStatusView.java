package com.github.h3yow.telemetrymetricsjava.alert;

import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;

/** 规则 × 序列 的当前状态视图（供查询）。 */
public record AlertStatusView(String ruleId, SeriesKey series, AlertState state, long updatedWindowStartMs,
                              double lastObservedValue, String thresholdMessage) {
}
