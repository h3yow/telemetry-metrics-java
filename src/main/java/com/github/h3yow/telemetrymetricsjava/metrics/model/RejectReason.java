package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 样本被拒绝的可区分原因。
 */
public enum RejectReason {
    /** 时间戳早于（当前最老可保留窗口 - 允许迟到范围），窗口已滚动淘汰。 */
    TOO_OLD,
    /** 时间戳超过当前处理时间 + 未来容忍上限。 */
    TOO_FAR_IN_FUTURE,
    /** 目标窗口内该序列样本条数已达容量上限。 */
    WINDOW_CAPACITY_EXCEEDED,
    /** 全局时间序列（指标名+标签组合）数量已达上限。 */
    TOO_MANY_SERIES,
    /** 标签基数（某个标签 key 的不同 value 数）超限。 */
    LABEL_CARDINALITY_EXCEEDED,
    /** 相同 sampleId 的样本已被计入（跨窗口边界 / 重复上报）。 */
    DUPLICATE_SAMPLE,
    /** 非法入参（指标名为空、时间戳非法等）。 */
    INVALID_ARGUMENT
}
