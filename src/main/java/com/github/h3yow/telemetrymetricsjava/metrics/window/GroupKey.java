package com.github.h3yow.telemetrymetricsjava.metrics.window;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 分组键：只包含参与分组的标签子集（按 key 排序、不可变）。
 *
 * <p>未参与分组的标签不进入分组键，因此同一取值组合的多条序列会合并成一组；
 * 分组键实现 {@link Comparable}，比较顺序按标签 key/value 字典序确定，
 * 保证分组结果的迭代/返回顺序稳定可复现。</p>
 */
public record GroupKey(Map<String, String> labels) implements Comparable<GroupKey> {

    /** 无分组标签时唯一的“总组”键。 */
    public static GroupKey empty() {
        return new GroupKey(Map.of());
    }

    /**
     * 从完整标签集中投影出 groupByKeys 对应的子标签，构造分组键。
     * 某个分组键在标签集中不存在时，按空字符串 "" 取值（“无此标签”自成一组，
     * 不与任何实际值合并）。groupByKeys 会先去重并按字典序排序，使结果与传入顺序无关。
     */
    public static GroupKey project(Map<String, String> labels, List<String> groupByKeys) {
        if (groupByKeys == null || groupByKeys.isEmpty()) {
            return empty();
        }
        TreeMap<String, String> projected = new TreeMap<>();
        for (String key : new java.util.TreeSet<>(groupByKeys)) {
            projected.put(key, labels.getOrDefault(key, ""));
        }
        return new GroupKey(Map.copyOf(projected));
    }

    /** 稳定、可解释的单行文本表示，如 {host=a,service=x}，用于事件与日志。 */
    public String render() {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : labels.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.append('}').toString();
    }

    /**
     * 自然顺序：逐项比较 (key,value) 字典序，键数量少者排前；
     * 空分组键排在最前。
     */
    @Override
    public int compareTo(GroupKey other) {
        var a = labels.entrySet().iterator();
        var b = other.labels.entrySet().iterator();
        while (a.hasNext() && b.hasNext()) {
            Map.Entry<String, String> ea = a.next();
            Map.Entry<String, String> eb = b.next();
            int k = ea.getKey().compareTo(eb.getKey());
            if (k != 0) {
                return k;
            }
            int v = ea.getValue().compareTo(eb.getValue());
            if (v != 0) {
                return v;
            }
        }
        return Integer.compare(labels.size(), other.labels.size());
    }
}
