package com.github.h3yow.telemetrymetricsjava.metrics.model;

import java.util.Map;

/**
 * 一个分组在单个窗口内的跨序列聚合结果（不可变快照）。
 *
 * <p>由同一窗口内分组标签取值相同的多条序列聚合而来：未参与分组的标签只用于
 * 合并、不出现在 {@link #groupLabels()} 中。{@code lastValue} 按事件时间定义。</p>
 *
 * @param name        指标名
 * @param groupLabels 分组标签（仅含调用方指定的分组键，按 key 排序）
 * @param windowStart 窗口起始 epoch millis
 * @param windowSize  窗口长度 millis
 * @param sampleCount 组内样本总数（跨序列）
 * @param seriesCount 贡献样本的序列数
 * @param sum         组内样本值之和
 * @param min         组内最小值
 * @param max         组内最大值
 * @param lastValue   组内按事件时间的最后值
 */
public record GroupWindowAggregation(String name, Map<String, String> groupLabels,
                                     long windowStart, long windowSize,
                                     long sampleCount, long seriesCount,
                                     double sum, double min, double max,
                                     double lastValue) {

    /** 平均值 = sum / sampleCount；无样本时为 NaN（正常查询不会出现空分组）。 */
    public double average() {
        return sampleCount == 0 ? Double.NaN : sum / sampleCount;
    }
}
