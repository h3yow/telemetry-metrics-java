package com.github.h3yow.telemetrymetricsjava.metrics;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 指标采集与告警的容量/时间上界配置。
 */
@ConfigurationProperties(prefix = "metrics")
public class MetricsProperties {

    private long windowSizeMs = 10_000;
    private int retainedWindows = 3;
    private int maxSamplesPerSeriesWindow = 10_000;
    private int maxSeries = 10_000;
    private int maxLabelValuesPerKey = 2_000;
    private long maxFutureSkewMs = 60_000;
    private long lateArrivalGraceMs = 5_000;
    private int maxGroupCombinations = 1_000;
    private int maxSeriesPerGroup = 5_000;
    private int maxQueryWindows = 1_000;
    private int groupAlertRetentionWindows = 6;

    public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long windowSizeMs) { this.windowSizeMs = windowSizeMs; }
    public int getRetainedWindows() { return retainedWindows; }
    public void setRetainedWindows(int retainedWindows) { this.retainedWindows = retainedWindows; }
    public int getMaxSamplesPerSeriesWindow() { return maxSamplesPerSeriesWindow; }
    public void setMaxSamplesPerSeriesWindow(int v) { this.maxSamplesPerSeriesWindow = v; }
    public int getMaxSeries() { return maxSeries; }
    public void setMaxSeries(int maxSeries) { this.maxSeries = maxSeries; }
    public int getMaxLabelValuesPerKey() { return maxLabelValuesPerKey; }
    public void setMaxLabelValuesPerKey(int v) { this.maxLabelValuesPerKey = v; }
    public long getMaxFutureSkewMs() { return maxFutureSkewMs; }
    public void setMaxFutureSkewMs(long v) { this.maxFutureSkewMs = v; }
    public long getLateArrivalGraceMs() { return lateArrivalGraceMs; }
    public void setLateArrivalGraceMs(long v) { this.lateArrivalGraceMs = v; }
    /** 一次分组查询/分组封定允许的标签取值组合数上限。 */
    public int getMaxGroupCombinations() { return maxGroupCombinations; }
    public void setMaxGroupCombinations(int v) { this.maxGroupCombinations = v; }
    /** 单个分组内允许合并的序列数上限。 */
    public int getMaxSeriesPerGroup() { return maxSeriesPerGroup; }
    public void setMaxSeriesPerGroup(int v) { this.maxSeriesPerGroup = v; }
    /** 一次查询允许请求的窗口数上限（from/to 对齐后）。 */
    public int getMaxQueryWindows() { return maxQueryWindows; }
    public void setMaxQueryWindows(int v) { this.maxQueryWindows = v; }
    /** 分组告警状态在分组消失后保留的窗口数；超出后丢弃状态条目（不补发事件）。 */
    public int getGroupAlertRetentionWindows() { return groupAlertRetentionWindows; }
    public void setGroupAlertRetentionWindows(int v) { this.groupAlertRetentionWindows = v; }
}
