package com.github.h3yow.telemetrymetricsjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 指标引擎的可调参数（application.properties 可用 telemetry.* 覆盖）。
 */
@ConfigurationProperties(prefix = "telemetry")
public class TelemetryProperties {

    /** 滚动窗口大小（毫秒） */
    private long windowSizeMs = 10_000;
    /** 单窗口最大样本数 */
    private int windowCapacity = 10_000;
    /** 每条序列保留的窗口数量（内存上界） */
    private int retainedWindowsPerSeries = 6;
    /** 最大序列数（基数上界） */
    private int maxSeries = 10_000;
    /** 允许的未来事件时间偏移（毫秒），超出拒绝 */
    private long maxFutureSkewMs = 60_000;
    /** 水位落后“现在”的延迟（毫秒），范围内迟到样本仍可进入未关闭窗口 */
    private long allowedLatenessMs = 1_000;
    /** 保留的告警事件数上界 */
    private int eventHistorySize = 1_000;
    /** 自动滚动周期（毫秒） */
    private long tickIntervalMs = 1_000;
    /** 是否启用容器内的自动滚动；测试中关闭，改用显式 tick 保持时间确定性 */
    private boolean autoTickEnabled = true;

    public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long v) { this.windowSizeMs = v; }
    public int getWindowCapacity() { return windowCapacity; }
    public void setWindowCapacity(int v) { this.windowCapacity = v; }
    public int getRetainedWindowsPerSeries() { return retainedWindowsPerSeries; }
    public void setRetainedWindowsPerSeries(int v) { this.retainedWindowsPerSeries = v; }
    public int getMaxSeries() { return maxSeries; }
    public void setMaxSeries(int v) { this.maxSeries = v; }
    public long getMaxFutureSkewMs() { return maxFutureSkewMs; }
    public void setMaxFutureSkewMs(long v) { this.maxFutureSkewMs = v; }
    public long getAllowedLatenessMs() { return allowedLatenessMs; }
    public void setAllowedLatenessMs(long v) { this.allowedLatenessMs = v; }
    public int getEventHistorySize() { return eventHistorySize; }
    public void setEventHistorySize(int v) { this.eventHistorySize = v; }
    public long getTickIntervalMs() { return tickIntervalMs; }
    public void setTickIntervalMs(long v) { this.tickIntervalMs = v; }
    public boolean isAutoTickEnabled() { return autoTickEnabled; }
    public void setAutoTickEnabled(boolean v) { this.autoTickEnabled = v; }
}
