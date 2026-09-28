package com.github.h3yow.telemetrymetricsjava.metrics.group;

import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitException;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitReason;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 把同一窗口内多条序列的聚合快照，按分组键取值合并为跨序列分组聚合（纯函数，无锁）。
 *
 * <p><b>分组语义</b>：只按 groupBy 声明的标签键取值分组，序列上的其它标签一律忽略，
 * 因此不会把同一组拆成多组；某序列缺少某个分组键时以空串作为取值（确定性的缺失值表示）。
 * groupBy 为空时全部序列归入唯一一个全局组。</p>
 *
 * <p><b>稳定性</b>：调用方应保证入参列表确定有序（存储层已按序列键排序）；
 * 本类用 {@link LinkedHashMap} 保持组的首次出现顺序，并在返回前按分组标签渲染排序，
 * 相同输入多次运行结果（顺序与内容）完全一致。</p>
 *
 * <p><b>容量边界</b>：不同分组组合数 ≤ maxCombinations、单组序列数 ≤ maxSeriesPerGroup，
 * 触限时抛出带 {@link GroupLimitReason} 的 {@link GroupLimitException}，
 * 绝不悄悄丢样本或合并不同组合。</p>
 */
public final class GroupAggregationBuilder {

    private GroupAggregationBuilder() {
    }

    /** 计算一条序列在指定 groupBy 下的分组标签（按 key 排序，缺失键取空串）。 */
    public static Map<String, String> groupLabelsOf(Map<String, String> seriesLabels,
                                                    List<String> groupBy) {
        TreeMap<String, String> groupLabels = new TreeMap<>();
        for (String key : groupBy) {
            groupLabels.put(key, seriesLabels.getOrDefault(key, ""));
        }
        return groupLabels;
    }

    /** 分组的稳定单行文本：{@code k1=v1,k2=v2}（按 key 排序）；全局组为 {@code *}。 */
    public static String renderGroup(Map<String, String> groupLabels) {
        if (groupLabels.isEmpty()) {
            return "*";
        }
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> e : groupLabels.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    /**
     * 为单个窗口构建全部分组聚合。
     *
     * @param name              指标名（写入结果）
     * @param groupBy           分组标签键（调用方已排序、去重）
     * @param windowStart       窗口起点 epoch millis
     * @param windowSize        窗口长度 millis
     * @param seriesWindows     该窗口内参与合并的各序列快照（应已按序列键稳定排序）
     * @param maxCombinations   允许的不同分组组合数上界
     * @param maxSeriesPerGroup 单个分组允许的序列条数上界
     * @return 按分组标签渲染稳定排序的分组聚合；无输入时为空列表（不凭空产生空分组）
     */
    public static List<GroupWindowAggregation> buildForWindow(String name, List<String> groupBy,
                                                              long windowStart, long windowSize,
                                                              List<WindowAggregation> seriesWindows,
                                                              int maxCombinations,
                                                              int maxSeriesPerGroup) {
        // 用渲染串作为组身份，LinkedHashMap 保留首次出现顺序，便于做确定性的容量触限判断。
        Map<String, Accumulator> byGroup = new LinkedHashMap<>();

        for (WindowAggregation agg : seriesWindows) {
            Map<String, String> groupLabels = groupLabelsOf(agg.labels(), groupBy);
            String groupKey = renderGroup(groupLabels);
            Accumulator acc = byGroup.get(groupKey);
            if (acc == null) {
                if (byGroup.size() >= maxCombinations) {
                    throw new GroupLimitException(GroupLimitReason.TOO_MANY_GROUP_COMBINATIONS,
                            maxCombinations, byGroup.size() + 1L,
                            "metric '" + name + "' group-by " + groupBy
                                    + " produced more than maxGroupCombinations="
                                    + maxCombinations + " distinct combinations");
                }
                acc = new Accumulator(new TreeMap<>(groupLabels));
                byGroup.put(groupKey, acc);
            }
            acc.merge(agg, groupKey, maxSeriesPerGroup);
        }

        List<GroupWindowAggregation> out = new ArrayList<>(byGroup.size());
        for (Map.Entry<String, Accumulator> e : byGroup.entrySet()) {
            out.add(e.getValue().toAggregation(name, windowStart, windowSize));
        }
        // 显式按分组键排序，使结果顺序不依赖 HashMap 迭代，完全可复现。
        out.sort(java.util.Comparator.comparing(g -> renderGroup(g.groupLabels())));
        return out;
    }

    /** 单个分组在单个窗口内的跨序列累加器。 */
    private static final class Accumulator {
        private final Map<String, String> groupLabels;
        private long sampleCount;
        private long seriesCount;
        private double sum;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double lastValue = Double.NaN;
        private long lastTimestamp = Long.MIN_VALUE;

        private Accumulator(Map<String, String> groupLabels) {
            this.groupLabels = groupLabels;
        }

        private void merge(WindowAggregation agg, String groupKey, int maxSeriesPerGroup) {
            if (seriesCount >= maxSeriesPerGroup) {
                throw new GroupLimitException(GroupLimitReason.TOO_MANY_SERIES_IN_GROUP,
                        maxSeriesPerGroup, seriesCount + 1L,
                        "group " + groupKey + " contains more than maxSeriesPerGroup="
                                + maxSeriesPerGroup + " distinct series");
            }
            seriesCount++;
            sampleCount += agg.count();
            sum += agg.sum();
            if (agg.min() < min) {
                min = agg.min();
            }
            if (agg.max() > max) {
                max = agg.max();
            }
            // 组内“最后值”同样按事件时间：取所有序列快照中事件时间最大的那个值。
            // 时间戳相等时保持先到序列的值（输入已按序列键排序），保证可复现。
            if (agg.lastTimestamp() > lastTimestamp) {
                lastTimestamp = agg.lastTimestamp();
                lastValue = agg.lastValue();
            }
        }

        private GroupWindowAggregation toAggregation(String name, long windowStart,
                                                     long windowSize) {
            return new GroupWindowAggregation(name, Map.copyOf(groupLabels),
                    windowStart, windowSize, sampleCount, seriesCount,
                    sum, min, max, lastValue);
        }
    }
}
