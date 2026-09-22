package com.github.h3yow.telemetrymetricsjava.model;

/**
 * 一条指标样本。{@code timestampMs} 为事件时间（由上报方提供），窗口归属按事件时间计算。
 *
 * @param name        指标名
 * @param tags        标签维度（不可变、可空值会被归一化为空串）
 * @param value       样本值
 * @param timestampMs 事件时间（epoch millis）
 * @param sampleId    幂等键，用于去重，避免并发/重试导致重复计数
 */
public record MetricSample(String name, java.util.Map<String, String> tags, double value, long timestampMs,
                           String sampleId) {
    public MetricSample {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        tags = tags == null ? java.util.Map.of() : java.util.Map.copyOf(tags);
        if (sampleId == null || sampleId.isBlank()) {
            throw new IllegalArgumentException("sampleId must not be blank");
        }
    }
}
