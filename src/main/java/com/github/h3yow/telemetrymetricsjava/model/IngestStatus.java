package com.github.h3yow.telemetrymetricsjava.model;

/**
 * 样本上报结果。每种非接收结果都是可区分的拒绝（或幂等跳过）原因。
 */
public enum IngestStatus {
    /** 已接收并计入窗口 */
    ACCEPTED,
    /** 事件时间早于允许的乱序下界（窗口已关闭），明确拒绝 */
    REJECTED_TOO_OLD,
    /** 事件时间超过未来容忍上界（防止远未来事件污染水位） */
    REJECTED_TOO_FUTURE,
    /** 目标窗口样本数达到容量上界，明确拒绝 */
    REJECTED_WINDOW_FULL,
    /** 序列数量达到全局上界，明确拒绝 */
    REJECTED_TOO_MANY_SERIES,
    /** 同一 sampleId 已经接收过，幂等跳过，不重复计数 */
    DUPLICATE
}
