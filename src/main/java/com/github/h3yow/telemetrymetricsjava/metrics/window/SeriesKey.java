package com.github.h3yow.telemetrymetricsjava.metrics.window;

import java.util.Map;
import java.util.TreeMap;

/**
 * 时间序列键：指标名 + 规范化（按 key 排序）后的标签集。
 * 标签以不同 Map 实现、不同迭代顺序传入时，序列身份保持一致。
 */
public record SeriesKey(String name, Map<String, String> labels) {

    public static SeriesKey of(String name, Map<String, String> labels) {
        return new SeriesKey(name, Map.copyOf(new TreeMap<>(labels)));
    }

    /** 稳定、可解释的单行文本表示，用于日志与告警事件的 seriesKey。 */
    public String render() {
        StringBuilder sb = new StringBuilder(name).append('{');
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
}
