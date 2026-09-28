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

    // —— 按标签分组的跨序列汇总 / 分组告警容量边界 ——
    /** 单次分组查询或单条分组规则，允许出现的不同分组键取值组合数上界。 */
    private int maxGroupCombinations = 500;
    /** 单个分组内允许的不同序列条数上界。 */
    private int maxSeriesPerGroup = 1_000;
    /** 一次分组查询返回的行数（分组数 × 窗口数）上界。 */
    private int maxGroupedQueryRows = 5_000;
    /** 一次分组查询请求覆盖的窗口数上界。 */
    private int maxQueryWindows = 100;
    /** 分组告警状态在分组无样本后仍保留的窗口数（保留期内回归不算新触发）。 */
    private int groupAlertRetentionWindows = 3;

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
    public int getMaxGroupCombinations() { return maxGroupCombinations; }
    public void setMaxGroupCombinations(int v) { this.maxGroupCombinations = v; }
    public int getMaxSeriesPerGroup() { return maxSeriesPerGroup; }
    public void setMaxSeriesPerGroup(int v) { this.maxSeriesPerGroup = v; }
    public int getMaxGroupedQueryRows() { return maxGroupedQueryRows; }
    public void setMaxGroupedQueryRows(int v) { this.maxGroupedQueryRows = v; }
    public int getMaxQueryWindows() { return maxQueryWindows; }
    public void setMaxQueryWindows(int v) { this.maxQueryWindows = v; }
    public int getGroupAlertRetentionWindows() { return groupAlertRetentionWindows; }
    public void setGroupAlertRetentionWindows(int v) { this.groupAlertRetentionWindows = v; }
}