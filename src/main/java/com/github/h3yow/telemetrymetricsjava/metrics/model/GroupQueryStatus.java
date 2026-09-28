package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 分组查询的完成状态。超限时给出<b>可区分</b>的明确反馈，
 * 不悄悄丢样本、也不把不同标签组合合并。
 */
public enum GroupQueryStatus {
    /** 查询完整完成，results 为全部命中分组的全部窗口。 */
    OK,
    /** 请求窗口数超过 maxQueryWindows 上限（本次查询未执行）。 */
    TOO_MANY_WINDOWS,
    /** 分组键取值组合数超过 maxGroupCombinations 上限（本次查询未执行）。 */
    TOO_MANY_GROUP_COMBINATIONS,
    /** 某个分组内合并的序列数超过 maxSeriesPerGroup 上限（本次查询未执行）。 */
    TOO_MANY_SERIES_IN_GROUP
}
