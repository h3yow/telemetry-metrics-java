package com.github.h3yow.telemetrymetricsjava.model;

/**
 * 一个滚动窗口内的聚合统计。窗口关闭后即为不可变快照。
 */
public record WindowStats(long windowStartMs, long windowEndMs, long count, double sum, double min, double max,
                          double lastValue) {

    public double avg() {
        return count == 0 ? 0.0 : sum / count;
    }
}
