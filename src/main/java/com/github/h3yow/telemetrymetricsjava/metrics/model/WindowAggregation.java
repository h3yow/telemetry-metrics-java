package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 单个序列在单个窗口内的聚合结果（不可变快照）。
 *
 * @param name        指标名
 * @param labels      标签
 * @param windowStart 窗口起始 epoch millis（左闭右开 [start, start+size)）
 * @param windowSize  窗口长度 millis
 * @param count         样本数
 * @param sum           样本值之和
 * @param min           最小值
 * @param max           最大值
 * @param lastValue     窗口内最后到达样本的值（按事件时间）
 * @param lastTimestamp lastValue 对应的事件时间（跨序列分组合并时用于确定全局最后值）
 */
public record WindowAggregation(String name, java.util.Map<String, String> labels,
                                long windowStart, long windowSize,
                                long count, double sum, double min, double max,
                                double lastValue, long lastTimestamp) {

    /** 兼容旧测试/调用点的构造方式：lastTimestamp 缺省为窗口起点。 */
    public WindowAggregation(String name, java.util.Map<String, String> labels,
                             long windowStart, long windowSize,
                             long count, double sum, double min, double max, double lastValue) {
        this(name, labels, windowStart, windowSize, count, sum, min, max, lastValue, windowStart);
    }

    public double average() {
        return count == 0 ? Double.NaN : sum / count;
    }
}
