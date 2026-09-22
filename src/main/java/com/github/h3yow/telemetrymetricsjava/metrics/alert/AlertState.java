package com.github.h3yow.telemetrymetricsjava.metrics.alert;

/**
 * 规则在某条序列上的状态。
 */
public enum AlertState {
    /** 未越线（含初始态与恢复态）。 */
    RESOLVED,
    /** 已越线、告警持续中。 */
    FIRING
}
