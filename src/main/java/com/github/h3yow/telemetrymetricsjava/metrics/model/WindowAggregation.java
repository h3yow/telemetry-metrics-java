package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 单个序列在单个窗口内的聚合结果（不可变快照）。
 *
 * @param name        指标名
 * @param labels      标签
 * @param windowStart 窗口起始 epoch millis（左闭右开 [start, start+size)）
 * @param windowSize  窗口长度 millis
 * @param count       样本数
 * @param sum         样本值之和
 * @param min         最小值
 * @param max         最大值
 * @param lastValue   窗口内最后到达样本的值（按事件时间）
 */
public record WindowAggregation(String name, java.util.Map<String, String> labels,
                                long windowStart, long windowSize,
                                long count, double sum, double min, double max,
                                double lastValue) {

    public double average() {
        return count == 0 ? Double.NaN : sum / count;
    }
}
