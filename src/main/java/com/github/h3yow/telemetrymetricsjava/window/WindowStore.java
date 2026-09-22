package com.github.h3yow.telemetrymetricsjava.window;

import com.github.h3yow.telemetrymetricsjava.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.model.IngestStatus;
import com.github.h3yow.telemetrymetricsjava.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.model.WindowStats;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 按序列管理滚动窗口的存储与生命周期。
 *
 * <p>核心语义：</p>
 * <ul>
 *   <li>窗口归属严格按样本事件时间：{@code start = floor(timestamp / size) * size}；</li>
 *   <li>水位 {@code watermark = now - allowedLateness}，结束时间 {@code <= watermark}
 *       的窗口被一次性关闭（seal），迟到样本以 {@code REJECTED_TOO_OLD} 拒绝；</li>
 *   <li>每条序列至多保留 {@code retainedWindowsPerSeries} 个窗口，窗口关闭时顺带淘汰，
 *       空序列被移除，因此内存占用 = O(maxSeries × retainedWindows × windowCapacity) 有上界；</li>
 *   <li>序列获取/窗口创建与水位滚动在同一把滚动锁内互斥，窗口写入在每序列锁内
 *       完成“去重/容量/累加”原子判定。</li>
 * </ul>
 */
public class WindowStore {

    private final long windowSizeMs;
    private final int windowCapacity;
    private final int retainedWindowsPerSeries;
    private final int maxSeries;
    private final long maxFutureSkewMs;
    private final long allowedLatenessMs;
    private final Clock clock;

    private final Map<SeriesKey, SeriesWindows> series = new ConcurrentHashMap<>();
    /**
     * 滚动锁：水位推进（seal+evict+发布水位）与 ingest 的序列获取/窗口创建决策都在该锁内，
     * 因此“seal 完成但水位尚未发布”的间隙里不会有样本创建出本该关闭的窗口，
     * 也不会出现空序列被淘汰后写入游离对象。窗口内的样本累加由每序列锁保证原子性。
     */
    private final Object rolloverLock = new Object();

    /** 单调递增的事件时间水位；endMs <= watermark 的窗口视为已关闭 */
    private volatile long watermark = Long.MIN_VALUE;

    public WindowStore(long windowSizeMs, int windowCapacity, int retainedWindowsPerSeries,
                       int maxSeries, long maxFutureSkewMs, long allowedLatenessMs, Clock clock) {
        if (windowSizeMs <= 0 || windowCapacity <= 0 || retainedWindowsPerSeries < 2 || maxSeries <= 0
                || maxFutureSkewMs < 0 || allowedLatenessMs < 0) {
            throw new IllegalArgumentException("invalid window parameters");
        }
        this.windowSizeMs = windowSizeMs;
        this.windowCapacity = windowCapacity;
        this.retainedWindowsPerSeries = retainedWindowsPerSeries;
        this.maxSeries = maxSeries;
        this.maxFutureSkewMs = maxFutureSkewMs;
        this.allowedLatenessMs = allowedLatenessMs;
        this.clock = clock;
    }

    public long windowSizeMs() {
        return windowSizeMs;
    }

    public long watermarkMs() {
        return watermark;
    }

    public int seriesCount() {
        return series.size();
    }

    private long windowStartFor(long timestampMs) {
        return Math.floorDiv(timestampMs, windowSizeMs) * windowSizeMs;
    }

    private boolean windowClosed(long windowStartMs) {
        return windowStartMs + windowSizeMs <= watermark;
    }

    public IngestResult ingest(MetricSample sample) {
        long now = clock.millis();
        if (sample.timestampMs() > now + maxFutureSkewMs) {
            return IngestResult.rejected(IngestStatus.REJECTED_TOO_FUTURE,
                    "event time " + sample.timestampMs() + " is more than " + maxFutureSkewMs
                            + "ms in the future (now=" + now + ")");
        }
        long windowStart = windowStartFor(sample.timestampMs());
        if (windowClosed(windowStart)) {
            return IngestResult.rejected(IngestStatus.REJECTED_TOO_OLD,
                    "window [" + windowStart + "," + (windowStart + windowSizeMs)
                            + ") already closed by watermark " + watermark);
        }

        SeriesKey key = SeriesKey.of(sample);
        // 序列获取、窗口创建与水位判定整体在滚动锁内，与 advanceWatermark 互斥，
        // 消除“seal 已完成但水位未发布”以及“空序列被淘汰后写入游离对象”两类竞态。
        synchronized (rolloverLock) {
            if (windowClosed(windowStart)) {
                return IngestResult.rejected(IngestStatus.REJECTED_TOO_OLD,
                        "window starting " + windowStart + " closed by concurrent rollover, watermark="
                                + watermark);
            }
            SeriesWindows sw = series.get(key);
            if (sw == null) {
                if (series.size() >= maxSeries) {
                    return IngestResult.rejected(IngestStatus.REJECTED_TOO_MANY_SERIES,
                            "series limit " + maxSeries + " reached");
                }
                sw = new SeriesWindows();
                series.put(key, sw);
            }
            return sw.ingest(windowStart, sample);
        }
    }

    public Optional<WindowStats> latestStats(SeriesKey key) {
        SeriesWindows sw = series.get(key);
        return sw == null ? Optional.empty() : sw.latestStats();
    }

    public List<WindowStats> statsBetween(SeriesKey key, long fromWindowStart, long toWindowStart) {
        SeriesWindows sw = series.get(key);
        return sw == null ? List.of() : sw.statsBetween(fromWindowStart, toWindowStart);
    }

    /**
     * 以 {@code nowMs} 推进水位，关闭所有 endMs <= watermark 的窗口并返回关闭事件。
     * 每个窗口恰好关闭一次，因此下游告警不会重复判定。
     */
    public List<SealedWindow> advanceWatermark(long nowMs) {
        long newWatermark = nowMs - allowedLatenessMs;
        List<SealedWindow> sealed = new ArrayList<>();
        if (newWatermark <= watermark) {
            return sealed;
        }
        // 全程持有滚动锁，与 ingest 的窗口创建互斥；
        // 水位最后发布，保证 ingest 不会看到“已 seal 但水位仍旧”。
        synchronized (rolloverLock) {
            if (newWatermark <= watermark) {
                return sealed;
            }
            for (Map.Entry<SeriesKey, SeriesWindows> e : series.entrySet()) {
                sealed.addAll(e.getValue().sealAndEvict(e.getKey(), newWatermark));
            }
            watermark = newWatermark;
            // 淘汰后窗口为空（含所有窗口都被 seal 并淘汰掉的序列）的序列整体移除
            series.entrySet().removeIf(e -> e.getValue().windowCount() == 0);
        }
        sealed.sort(Comparator.comparingLong((SealedWindow sw) -> sw.stats().windowStartMs())
                .thenComparing(sw -> sw.key().canonical()));
        return sealed;
    }

    /**
     * 水位推进产生的已关闭窗口事件，供告警引擎消费。
     */
    public record SealedWindow(SeriesKey key, WindowStats stats) {
    }

    /** 一条序列的窗口集合：按窗口起点有序，全部操作在实例锁内串行化。 */
    private final class SeriesWindows {
        private final ConcurrentSkipListMap<Long, TumblingWindow> windows = new ConcurrentSkipListMap<>();

        synchronized IngestResult ingest(long windowStart, MetricSample sample) {
            TumblingWindow w = windows.get(windowStart);
            if (w == null) {
                // 调用方持有滚动锁并已判定过水位；窗口只可能在首次出现时创建
                w = new TumblingWindow(windowStart, windowSizeMs, windowCapacity);
                windows.put(windowStart, w);
            }
            TumblingWindow.AddResult r = w.addSample(sample.sampleId(), sample.value());
            return switch (r) {
                case ADDED -> IngestResult.accepted(windowStart);
                case DUPLICATE -> IngestResult.duplicate(windowStart);
                case FULL -> IngestResult.rejected(IngestStatus.REJECTED_WINDOW_FULL,
                        "window starting " + windowStart + " reached capacity " + windowCapacity);
                case SEALED -> IngestResult.rejected(IngestStatus.REJECTED_TOO_OLD,
                        "window starting " + windowStart + " is sealed");
            };
        }

        synchronized Optional<WindowStats> latestStats() {
            Map.Entry<Long, TumblingWindow> last = windows.lastEntry();
            if (last == null) {
                return Optional.empty();
            }
            return Optional.ofNullable(last.getValue().snapshot());
        }

        synchronized List<WindowStats> statsBetween(long fromStart, long toStart) {
            List<WindowStats> out = new ArrayList<>();
            for (TumblingWindow w : windows.subMap(fromStart, true, toStart, true).values()) {
                WindowStats s = w.snapshot();
                if (s != null) {
                    out.add(s);
                }
            }
            return out;
        }

        synchronized int windowCount() {
            return windows.size();
        }

        synchronized List<SealedWindow> sealAndEvict(SeriesKey key, long newWatermark) {
            List<SealedWindow> out = new ArrayList<>();
            // 关闭所有 endMs <= 新水位的窗口，每个窗口只 seal 一次
            for (TumblingWindow w : windows.values()) {
                if (!w.isSealed() && w.endMs() <= newWatermark) {
                    WindowStats stats = w.snapshot();
                    w.seal();
                    if (stats != null) {
                        out.add(new SealedWindow(key, stats));
                    }
                }
            }
            // 有界淘汰：仅保留最近 retainedWindowsPerSeries 个窗口
            while (windows.size() > retainedWindowsPerSeries) {
                windows.pollFirstEntry();
            }
            return out;
        }
    }
}
