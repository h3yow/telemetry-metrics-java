package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 单条指标样本。
 *
 * @param name      指标名
 * @param value     样本值
 * @param timestamp 事件时间（epoch millis）
 * @param labels    标签维度（不可变）
 * @param sampleId  幂等 ID（可空，用于跨窗口边界去重）
 */
public record MetricSample(String name, double value, long timestamp,
                           java.util.Map<String, String> labels, String sampleId) {

    public MetricSample {
        labels = labels == null ? java.util.Map.of() : java.util.Map.copyOf(labels);
    }
}
