package com.github.h3yow.telemetrymetricsjava.metrics.web;

import java.util.List;

/**
 * 分组聚合查询请求。fromEpochMs/toEpochMs 会被对齐到窗口边界。
 *
 * @param name        指标名
 * @param groupByKeys 参与分组的标签键；空列表表示把该指标全部序列合并成一个总组
 * @param fromEpochMs 起始时间（含，对齐到窗口起点）
 * @param toEpochMs   结束时间（含，对齐到窗口起点）
 */
public record GroupQueryRequest(String name, List<String> groupByKeys,
                                long fromEpochMs, long toEpochMs) {
}
