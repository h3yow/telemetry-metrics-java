package com.github.h3yow.telemetrymetricsjava.metrics.web;

import java.util.List;

/**
 * 分组聚合查询请求。fromEpochMs/toEpochMs 会被对齐到窗口边界。
 *
 * @param name      指标名
 * @param groupBy   参与分组的标签键（空表示整指标一个组）
 * @param fromEpochMs 范围起点（含）
 * @param toEpochMs   范围终点（含，按窗口起点闭区间）
 */
public record GroupQueryRequest(String name, List<String> groupBy,
                                long fromEpochMs, long toEpochMs) {
}
