package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 跨序列分组聚合工具：把同一窗口内多条序列的单序列快照，按分组标签投影合并为
 * 分组聚合。纯函数、无共享状态，并发调用安全。
 *
 * <p>合并语义：sampleCount 为各序列 count 之和；seriesCount 为贡献序列数；
 * sum/min/max 跨序列合并；lastValue 取“事件时间最大”的样本值，事件时间相同
 * 时取序列键字典序较大者（入参已按序列键排序，即后出现的成员），保证结果
 * 确定可复现、不依赖到达顺序。</p>
 */
public final class GroupWindowAggregator {

    private GroupWindowAggregator() {
    }

    /**
     * 把同一窗口内的多条序列快照聚合成一个分组结果。
     *
     * @param groupKey     目标分组键（已投影）
     * @param windowStart  窗口起点
     * @param windowSizeMs 窗口长度
     * @param members      组内序列快照（至少一条，均有样本，按序列键升序）
     */
    public static GroupWindowAggregation merge(GroupKey groupKey, long windowStart, long windowSizeMs,
                                               List<WindowAggregation> members) {
        if (members == null || members.isEmpty()) {
            throw new IllegalArgumentException("group members must not be empty");
        }
        String name = members.get(0).name();
        long sampleCount = 0;
        double sum = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double lastValue = Double.NaN;
        long lastTimestamp = Long.MIN_VALUE;

        for (WindowAggregation m : members) {
            sampleCount += m.count();
            sum += m.sum();
            if (m.min() < min) {
                min = m.min();
            }
            if (m.max() > max) {
                max = m.max();
            }
            // 成员按序列键升序：事件时间更大者胜；时间相同则后者（字典序更大）胜。
            if (m.lastTimestamp() >= lastTimestamp) {
                lastTimestamp = m.lastTimestamp();
                lastValue = m.lastValue();
            }
        }
        return new GroupWindowAggregation(name, groupKey.labels(), windowStart, windowSizeMs,
                sampleCount, members.size(), sum, min, max, lastValue);
    }

    /**
     * 按分组键把同窗口的序列快照分桶。返回的 Map 以 {@link GroupKey} 自然顺序排序；
     * 每个桶内保持入参顺序（调用方应按序列键排序后传入）。
     */
    public static Map<GroupKey, List<WindowAggregation>> partition(
            List<WindowAggregation> windowSeries, List<String> groupByKeys) {
        TreeMap<GroupKey, List<WindowAggregation>> buckets = new TreeMap<>();
        for (WindowAggregation agg : windowSeries) {
            GroupKey key = GroupKey.project(agg.labels(), groupByKeys);
            buckets.computeIfAbsent(key, k -> new ArrayList<>()).add(agg);
        }
        return buckets;
    }
}
