package com.github.h3yow.telemetrymetricsjava.metrics.window;

import java.util.Map;
import java.util.TreeMap;

/**
 * 时间序列键：指标名 + 规范化（按 key 排序）后的标签集。
 * 标签以不同 Map 实现、不同迭代顺序传入时，序列身份保持一致。
 *
 * <p>自然顺序：先按指标名字典序，再按标签 (key,value) 逐项字典序，
 * 用于分组聚合时稳定的序列遍历顺序。</p>
 */
public record SeriesKey(String name, Map<String, String> labels)
        implements Comparable<SeriesKey> {

    public static SeriesKey of(String name, Map<String, String> labels) {
        return new SeriesKey(name, Map.copyOf(new TreeMap<>(labels)));
    }

    @Override
    public int compareTo(SeriesKey o) {
        int byName = this.name.compareTo(o.name);
        if (byName != 0) {
            return byName;
        }
        var a = this.labels.entrySet().iterator();
        var b = o.labels.entrySet().iterator();
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
        return Integer.compare(this.labels.size(), o.labels.size());
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
