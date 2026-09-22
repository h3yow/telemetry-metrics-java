package com.github.h3yow.telemetrymetricsjava.window;

import com.github.h3yow.telemetrymetricsjava.model.WindowStats;

/**
 * 单个滚动窗口 [startMs, startMs+sizeMs) 的聚合累加器。
 *
 * <p>线程安全：所有读写均在本对象监视器上串行化。样本通过 {@code sampleId} 幂等去重，
 * 去重集合大小受 {@code capacity} 约束；窗口关闭（seal）后清空去重集合释放内存，
 * 因为关闭后的窗口不再接收任何样本（迟到样本由 WindowStore 以 TOO_OLD 拒绝）。</p>
 */
public class TumblingWindow {

    /** addSample 的原子判定结果 */
    public enum AddResult {
        ADDED, DUPLICATE, FULL, SEALED
    }

    private final long startMs;
    private final long sizeMs;
    private final int capacity;

    private boolean sealed;
    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double lastValue;

    private final java.util.Set<String> sampleIds = new java.util.HashSet<>();

    public TumblingWindow(long startMs, long sizeMs, int capacity) {
        if (sizeMs <= 0) {
            throw new IllegalArgumentException("sizeMs must be positive");
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.startMs = startMs;
        this.sizeMs = sizeMs;
        this.capacity = capacity;
    }

    public long startMs() {
        return startMs;
    }

    public long endMs() {
        return startMs + sizeMs;
    }

    public synchronized boolean isSealed() {
        return sealed;
    }

    public synchronized int sampleCount() {
        return (int) count;
    }

    public int capacity() {
        return capacity;
    }

    public synchronized boolean containsSample(String sampleId) {
        return sampleIds.contains(sampleId);
    }

    /**
     * 原子地完成“去重判定 + 容量判定 + 聚合累加”，并发上报下不会重复计数也不会丢样本。
     */
    public synchronized AddResult addSample(String sampleId, double value) {
        if (sealed) {
            return AddResult.SEALED;
        }
        if (!sampleIds.add(sampleId)) {
            return AddResult.DUPLICATE;
        }
        if (count >= capacity) {
            // 回滚占位：容量满时不得把 sampleId 留在集合里
            sampleIds.remove(sampleId);
            return AddResult.FULL;
        }
        count++;
        sum += value;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        lastValue = value;
        return AddResult.ADDED;
    }

    /**
     * 关闭窗口：之后拒绝一切写入，并释放去重集合占用的内存。
     */
    public synchronized void seal() {
        sealed = true;
        sampleIds.clear();
    }

    /** 当前状态的不可变快照；空窗口返回 null 由调用方决定语义。 */
    public synchronized WindowStats snapshot() {
        if (count == 0) {
            return null;
        }
        return new WindowStats(startMs, startMs + sizeMs, count, sum, min, max, lastValue);
    }
}
