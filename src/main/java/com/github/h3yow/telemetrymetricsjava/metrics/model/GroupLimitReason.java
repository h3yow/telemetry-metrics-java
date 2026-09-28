package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 分组能力容量越界的可区分原因。分组统计不悄悄丢样本、不合并不同组合，
 * 一旦触及配置上界即以该枚举明确反馈，调用方可据此区分是哪一类超限。
 */
public enum GroupLimitReason {
    /** 查询/评估命中的不同分组键取值组合数超过 maxGroupCombinations。 */
    TOO_MANY_GROUP_COMBINATIONS,
    /** 单个分组内的不同序列条数超过 maxSeriesPerGroup。 */
    TOO_MANY_SERIES_IN_GROUP,
    /** 一次分组查询返回的分组行数（分组数 × 窗口数）超过 maxGroupedQueryRows。 */
    TOO_MANY_RESULT_ROWS,
    /** 一次分组查询请求的窗口数超过 maxQueryWindows。 */
    TOO_MANY_QUERY_WINDOWS
}
