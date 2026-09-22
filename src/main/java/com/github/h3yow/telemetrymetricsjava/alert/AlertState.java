package com.github.h3yow.telemetrymetricsjava.alert;

/**
 * 规则对某条序列的判定状态。状态机：OK -> FIRING -> RECOVERED ->(下次评估) OK ...
 */
public enum AlertState {
    /** 正常，未越线 */
    OK,
    /** 告警中（首次越线，或恢复后再次越线） */
    FIRING,
    /** 本窗口已恢复，事件已通知；下一窗口无变化则回归 OK */
    RECOVERED
}
