package com.github.h3yow.telemetrymetricsjava.model;

import java.util.Map;
import java.util.TreeMap;

/**
 * 指标名 + 排序后的标签集合，唯一标识一条时间序列。标签的存储顺序被归一化，
 * 保证同一逻辑维度无论上报顺序如何都聚合到同一条序列。
 */
public record SeriesKey(String name, Map<String, String> tags) {

    public SeriesKey(String name, Map<String, String> tags) {
        this.name = name;
        var sorted = new TreeMap<String, String>();
        if (tags != null) {
            tags.forEach((k, v) -> sorted.put(k, v == null ? "" : v));
        }
        this.tags = Map.copyOf(sorted);
    }

    public static SeriesKey of(MetricSample s) {
        return new SeriesKey(s.name(), s.tags());
    }

    public String canonical() {
        return name + tags;
    }
}
