package com.github.h3yow.telemetrymetricsjava.alert;

import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;

/**
 * 状态发生迁移时产生的事件（仅在 FIRING / RECOVERED 迁移点产生）。
 *
 * @param kind      FIRING 或 RECOVERED
 * @param ruleId    规则 ID
 * @param series    序列
 * @param windowStartMs 判定所依据的窗口起点
 * @param observedValue 判定时聚合量的实际值
 * @param thresholdMessage 可解释的判定依据描述
 */
public record AlertEvent(Kind kind, String ruleId, SeriesKey series, long windowStartMs,
                         double observedValue, String thresholdMessage) {

    public enum Kind {
        FIRING, RECOVERED
    }
}
