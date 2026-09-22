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
}
