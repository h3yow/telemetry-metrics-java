package com.github.h3yow.telemetrymetricsjava.metrics.model;

import java.util.Map;

/**
 * 单个分组在单个窗口内的跨序列聚合结果（不可变快照）。
 *
 * @param name        指标名
 * @param groupLabels 分组标签（只包含调用方指定的分组键，按 key 排序）
 * @param windowStart 窗口起始 epoch millis
 * @param windowSize  窗口长度 millis
 * @param sampleCount 组内全部序列的样本总数
 * @param seriesCount 组内序列条数（不同完整标签集的条数）
 * @param sum         组内样本值之和
 * @param min         组内最小值
 * @param max         组内最大值
 * @param lastValue   组内按事件时间的最后一个样本值
 */
public record GroupWindowAggregation(String name, Map<String, String> groupLabels,
                                     long windowStart, long windowSize,
                                     long sampleCount, long seriesCount,
                                     double sum, double min, double max,
                                     double lastValue) {

    /** 组内样本平均值；无样本为 NaN。 */
    public double average() {
        return sampleCount == 0 ? Double.NaN : sum / sampleCount;
    }
}
