package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.HashSet;
import java.util.Set;

/**
 * 单个时间序列在单个固定窗口 [windowStart, windowStart+windowSize) 内的累加器。
 *
 * <p>线程安全由 {@link MetricWindowStore} 的单一锁保证，本类自身不加锁，
 * 以保证“校验容量 -> 计入样本”在存储层是原子的，避免并发下超卖与重复计数。</p>
 */
final class SeriesWindow {

    private final String name;
    private final java.util.Map<String, String> labels;
    private final long windowStart;
    private final long windowSize;

    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double lastValue = Double.NaN;
    private long lastTimestamp = Long.MIN_VALUE;
    private boolean sealed;

    /** 已计入样本的幂等 ID 集合，容量上界等于样本条数（存储层已限制）。 */
    private final Set<String> seenSampleIds = new HashSet<>();

    SeriesWindow(String name, java.util.Map<String, String> labels, long windowStart, long windowSize) {
        this.name = name;
        this.labels = labels;
        this.windowStart = windowStart;
        this.windowSize = windowSize;
    }

    long windowStart() {
        return windowStart;
    }

    long count() {
        return count;
    }

    /**
     * 窗口是否已封定。封定后迟到样本仍可计入（在迟到宽限内），
     * 但该窗口聚合只允许被告警判定器取走一次（见 {@link MetricWindowStore}）。
     */
    boolean isSealed() {
        return sealed;
    }

    void markSealed() {
        this.sealed = true;
    }

    /**
     * 若该 sampleId 已计入本窗口则返回 false（跨窗口边界重复上报 / 重传）。
     * sampleId 为 null 时不做幂等判断。
     */
    boolean isDuplicate(String sampleId) {
        return sampleId != null && seenSampleIds.contains(sampleId);
    }

    /** 累加一条样本；调用方必须已完成容量/重复校验。 */
    void add(double value, long timestamp, String sampleId) {
        count++;
        sum += value;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        // lastValue 按事件时间定义，而非到达时间：乱序到达不会污染“最后一个值”。
        if (timestamp >= lastTimestamp) {
            lastTimestamp = timestamp;
            lastValue = value;
        }
        if (sampleId != null) {
            seenSampleIds.add(sampleId);
        }
    }

    /**
     * 生成不可变聚合快照。空窗口（count==0）返回 null，表示该窗口无样本。
     */
    WindowAggregation snapshot() {
        if (count == 0) {
            return null;
        }
        return new WindowAggregation(name, java.util.Map.copyOf(labels), windowStart, windowSize,
                count, sum, min, max, lastValue);
    }
}
