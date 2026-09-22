package com.github.h3yow.telemetrymetricsjava.metrics.web;

import java.util.Map;

/**
 * 聚合查询请求。fromEpochMs/toEpochMs 会被对齐到窗口边界。
 */
public record QueryRequest(String name, Map<String, String> labels,
                           long fromEpochMs, long toEpochMs) {
}
