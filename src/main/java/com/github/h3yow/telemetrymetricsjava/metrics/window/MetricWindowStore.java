package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.RejectReason;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存滚动窗口存储。
 *
 * <p><b>窗口模型</b>：固定（tumbling）窗口，窗口边界为
 * {@code floor(timestamp / windowSizeMs) * windowSizeMs}（epoch 对齐），
 * 区间左闭右开。样本一律按<b>事件时间</b>归入窗口，乱序/迟到样本落入其本来的窗口，
 * 绝不会跨越边界被重复计入。</p>
 *
 * <p><b>并发模型</b>：单一 {@link ReentrantLock} 串行化所有写入与维护动作，
 * “容量校验 / 重复校验 / 计入样本 / 建序列”是一个原子临界区，杜绝并发超卖、
 * 重复计数与样本丢失；查询在同一把锁下取不可变快照，随后锁外组装结果，
 * 查询延迟只与命中的窗口数（有界）有关。</p>
 *
 * <p><b>封定批次的有序、恰好一次投递</b>：封定快照在存储锁内收集进一个按窗口起点
 * 排序的共享待投队列，锁释放后由一把独立的<b>投递锁</b>串行化回调——拿到投递锁的
 * 线程会把队列中<b>所有</b>待投窗口（含其它并发线程刚封定并入队的窗口）按窗口起点
 * 升序一次性排空调走。因此即便多个线程并发封定（较晚窗口先完成存储锁临界区），
 * 分组告警侧看到的批次顺序也严格按窗口起点升序，且每个窗口恰好一次。此外，封定
 * 水位向前推进时，没有任何样本的“空窗口”也会以空批次补位投递，保证窗口序列对
 * 消费方是无空洞、无重复、单调递增的，保留期过期判定不依赖“恰好有数据的窗口”。</p>
 *
 * <p><b>内存上界</b>：任一时刻存活窗口数至多为
 * {@code retainedWindows + futureToleranceWindows}（时间在走，维护会持续淘汰）；
 * 序列数 ≤ maxSeries；单序列单窗口样本数 ≤ maxSamplesPerSeriesWindow；
 * 标签值基数 ≤ maxLabelValuesPerKey；幂等集合大小 ≤ 样本条数。总占用有明确上界：
 * {@code O(maxSeries × 窗口数 × 单窗口样本容量)}。</p>
 */
public class MetricWindowStore {

    private final long windowSizeMs;
    private final int retainedWindows;
    private final int maxSamplesPerSeriesWindow;
    private final int maxSeries;
    private final int maxLabelValuesPerKey;
    private final long maxFutureSkewMs;
    private final long lateArrivalGraceMs;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();

    /** seriesKey -> (windowStart -> 窗口累加器)。内层 TreeMap 便于按窗口淘汰与范围查询。 */
    private final Map<SeriesKey, TreeMap<Long, SeriesWindow>> series = new HashMap<>();

    /** labelKey -> 已出现过的 labelValue 集合，用于限制标签基数。 */
    private final Map<String, java.util.Set<String>> labelValues = new HashMap<>();

    private SealedWindowListener sealedWindowListener = aggregation -> { };

    /**
     * 已封定、待批量回调的窗口批次（windowStart -> 该窗口全部有样本序列的快照）。
     * 只在 {@link #lock} 内写入/合并；空窗口以空 List 补位。
     */
    private final TreeMap<Long, List<WindowAggregation>> pendingBatches = new TreeMap<>();

    /** 已补位投递到的最大窗口起点；下一个应投递的窗口起点 = 它 + windowSizeMs。 */
    private long lastTickedWindow = Long.MIN_VALUE;

    /**
     * 独立于 {@link #lock} 的投递锁：保证批量回调在任何并发下都<b>串行且升序</b>。
     * 绝不在持有 {@link #lock} 时获取它（回调不重入存储，避免锁序风险）。
     */
    private final ReentrantLock dispatchLock = new ReentrantLock();

    public MetricWindowStore(long windowSizeMs, int retainedWindows, int maxSamplesPerSeriesWindow,
                             int maxSeries, int maxLabelValuesPerKey, long maxFutureSkewMs,
                             long lateArrivalGraceMs, Clock clock) {
        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("windowSizeMs must be positive");
        }
        if (retainedWindows <= 0) {
            throw new IllegalArgumentException("retainedWindows must be positive");
        }
        if (maxSamplesPerSeriesWindow <= 0 || maxSeries <= 0 || maxLabelValuesPerKey <= 0) {
            throw new IllegalArgumentException("capacity limits must be positive");
        }
        if (maxFutureSkewMs < 0 || lateArrivalGraceMs < 0) {
            throw new IllegalArgumentException("skew/grace must not be negative");
        }
        this.windowSizeMs = windowSizeMs;
        this.retainedWindows = retainedWindows;
        this.maxSamplesPerSeriesWindow = maxSamplesPerSeriesWindow;
        this.maxSeries = maxSeries;
        this.maxLabelValuesPerKey = maxLabelValuesPerKey;
        this.maxFutureSkewMs = maxFutureSkewMs;
        this.lateArrivalGraceMs = lateArrivalGraceMs;
        this.clock = clock;
    }

    public void setSealedWindowListener(SealedWindowListener listener) {
        lock.lock();
        try {
            this.sealedWindowListener = listener == null ? aggregation -> { } : listener;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 接收一条样本。返回带明确接受/拒绝结论与可解释原因的结果。
     */
    public IngestResult ingest(MetricSample sample) {
        if (sample == null || sample.name() == null || sample.name().isBlank()) {
            return IngestResult.rejected(RejectReason.INVALID_ARGUMENT,
                    "sample and metric name must be present");
        }
        if (Double.isNaN(sample.value()) || Double.isInfinite(sample.value())) {
            return IngestResult.rejected(RejectReason.INVALID_ARGUMENT,
                    "value must be finite, got " + sample.value());
        }
        if (sample.labels() == null) {
            return IngestResult.rejected(RejectReason.INVALID_ARGUMENT, "labels must not be null");
        }
        long ts = sample.timestamp();
        long now = clock.millis();
        long windowStart = floorWindowStart(ts);

        lock.lock();
        try {
            // 维护先行：保证存活窗口集合始终是有界的。
            maintainWindowsLocked(now);

            long cutoff = now - lateArrivalGraceMs;
            if (ts < cutoff) {
                return IngestResult.rejected(RejectReason.TOO_OLD,
                        "timestamp=" + ts + " is older than late-arrival grace cutoff=" + cutoff
                                + " (now=" + now + ", graceMs=" + lateArrivalGraceMs + ")");
            }
            if (ts > now + maxFutureSkewMs) {
                return IngestResult.rejected(RejectReason.TOO_FAR_IN_FUTURE,
                        "timestamp=" + ts + " exceeds now+maxFutureSkew="
                                + (now + maxFutureSkewMs) + " (now=" + now + ")");
            }

            SeriesKey key = SeriesKey.of(sample.name(), sample.labels());
            TreeMap<Long, SeriesWindow> windows = series.get(key);

            // 新序列需要在计入前做全局容量与标签基数校验。
            if (windows == null) {
                if (series.size() >= maxSeries) {
                    return IngestResult.rejected(RejectReason.TOO_MANY_SERIES,
                            "global series limit reached: maxSeries=" + maxSeries);
                }
                for (Map.Entry<String, String> e : sample.labels().entrySet()) {
                    java.util.Set<String> values =
                            labelValues.computeIfAbsent(e.getKey(), k -> new java.util.HashSet<>());
                    if (!values.contains(e.getValue()) && values.size() >= maxLabelValuesPerKey) {
                        return IngestResult.rejected(RejectReason.LABEL_CARDINALITY_EXCEEDED,
                                "label '" + e.getKey() + "' cardinality limit reached: max="
                                        + maxLabelValuesPerKey);
                    }
                }
                windows = new TreeMap<>();
                series.put(key, windows);
                for (Map.Entry<String, String> e : sample.labels().entrySet()) {
                    labelValues.computeIfAbsent(e.getKey(), k -> new java.util.HashSet<>())
                            .add(e.getValue());
                }
            }

            SeriesWindow w = windows.get(windowStart);
            if (w == null) {
                w = new SeriesWindow(sample.name(), sample.labels(), windowStart, windowSizeMs);
                windows.put(windowStart, w);
            }
            if (w.isDuplicate(sample.sampleId())) {
                return IngestResult.rejected(RejectReason.DUPLICATE_SAMPLE,
                        "sampleId=" + sample.sampleId() + " already counted in window starting "
                                + windowStart);
            }
            if (w.count() >= maxSamplesPerSeriesWindow) {
                return IngestResult.rejected(RejectReason.WINDOW_CAPACITY_EXCEEDED,
                        "series=" + key.render() + " window=" + windowStart
                                + " reached maxSamplesPerSeriesWindow="
                                + maxSamplesPerSeriesWindow);
            }

            w.add(sample.value(), ts, sample.sampleId());
            return IngestResult.accepted(windowStart,
                    "ingested into window [" + windowStart + ", " + (windowStart + windowSizeMs)
                            + "), count=" + w.count());
        } finally {
            lock.unlock();
            dispatchPendingBatches();
        }
    }

    /**
     * 窗口维护：封定越过迟到宽限的窗口（每窗口只回调一次），并淘汰超出保留范围的窗口。
     * 可由定时任务或测试显式触发；ingest 路径也会顺带执行。
     */
    public void maintain() {
        lock.lock();
        try {
            maintainWindowsLocked(clock.millis());
        } finally {
            lock.unlock();
            dispatchPendingBatches();
        }
    }

    /**
     * 串行、升序、恰好一次地投递全部待投封定批次。先抢存储锁把本线程新封定的窗口并入
     * 共享队列，再抢投递锁；抢到投递锁后重新在存储锁内合并一次（收编等待期间其它线程
     * 新封定的窗口），随后锁外按窗口起点升序把队列排空调走。
     */
    private void dispatchPendingBatches() {
        dispatchLock.lock();
        try {
            NavigableMap<Long, List<WindowAggregation>> ready = new TreeMap<>();
            lock.lock();
            try {
                ready.putAll(pendingBatches);
                pendingBatches.clear();
            } finally {
                lock.unlock();
            }
            // ready 为升序 TreeMap：窗口处理顺序与各线程封定完成的先后无关。
            for (Map.Entry<Long, List<WindowAggregation>> e : ready.entrySet()) {
                sealedWindowListener.onWindowBatchSealed(e.getKey(), e.getValue());
            }
        } finally {
            dispatchLock.unlock();
        }
    }

    private void maintainWindowsLocked(long now) {
        // 窗口 [s, s+size) 封定条件：s + size + grace <= now，故封定上界为
        // floor((now - grace - size) / size) * size。grace=0 时恰好在下一个窗口开始时封定。
        long sealHorizon = floorWindowStart(now - lateArrivalGraceMs - windowSizeMs);
        long oldestAllowed = floorWindowStart(now) - (long) (retainedWindows - 1) * windowSizeMs;

        // 本趟第一个新封定窗口的起点（用于从上次投递水位连续补位空窗口）。
        long firstSealedThisRun = Long.MAX_VALUE;

        // 本趟新封定的快照按 windowStart 合并进共享待投队列；同一窗口来自所有序列的快照
        // 组成一个稳定有序批次（按序列键排序），交给分组告警做跨序列聚合。
        Iterator<Map.Entry<SeriesKey, TreeMap<Long, SeriesWindow>>> it = series.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<SeriesKey, TreeMap<Long, SeriesWindow>> entry = it.next();
            TreeMap<Long, SeriesWindow> windows = entry.getValue();

            // 1) 封定：窗口结束时间 + 宽限已过，且本窗口尚未封定 -> 回调一次聚合快照。
            for (SeriesWindow w : windows.headMap(sealHorizon, true).values()) {
                if (!w.isSealed()) {
                    w.markSealed();
                    WindowAggregation agg = w.snapshot();
                    if (agg != null) {
                        // 单序列回调仍在锁内触发（只读取快照、不重入存储）。
                        sealedWindowListener.onWindowSealed(agg);
                        List<WindowAggregation> batch =
                                pendingBatches.computeIfAbsent(agg.windowStart(), k -> new ArrayList<>());
                        batch.add(agg);
                        if (agg.windowStart() < firstSealedThisRun) {
                            firstSealedThisRun = agg.windowStart();
                        }
                    }
                }
            }

            // 2) 淘汰：早于保留下界的窗口直接移除（聚合已在封定时被告警侧消费）。
            windows.headMap(oldestAllowed, false).clear();
            if (windows.isEmpty()) {
                it.remove();
            }
        }

        // 从上次投递水位的下一窗口开始，一直补到本趟封定水位：缺失窗口以空批次补位，
        // 保证消费方看到的窗口序列连续无空洞——即使本趟在更晚窗口才封定到新数据，中间
        // 的空窗口也不会被略过。缺样本窗口不是 0 值，对分组判定仅表示“这一拍没有该组
        // 数据”（不推进状态、不过期），但它让消费方的有序处理不必永久等待空洞。
        // 首次有封定数据时从本趟最早数据窗口起步（更早窗口无数据也无状态，无需补位）。
        long fillFrom = lastTickedWindow == Long.MIN_VALUE
                ? firstSealedThisRun
                : lastTickedWindow + windowSizeMs;
        for (long s = fillFrom; s <= sealHorizon; s += windowSizeMs) {
            // 已存在（本趟封定的真实数据批次）不会被空批次覆盖。
            pendingBatches.computeIfAbsent(s, k -> List.of());
        }
        if (fillFrom <= sealHorizon && sealHorizon > lastTickedWindow) {
            // 本趟保证 sealHorizon 键已入队（真实数据或空补位），水位推进到它。
            lastTickedWindow = sealHorizon;
        }

        // 按序列键对每个批次排序，保证分组聚合输入顺序确定、结果可复现。空批次为不可变
        // 空 List，无需排序。
        for (List<WindowAggregation> batch : pendingBatches.values()) {
            if (batch.isEmpty()) {
                continue;
            }
            batch.sort(java.util.Comparator.comparing(
                    a -> SeriesKey.of(a.name(), a.labels()).render()));
        }
    }

    /**
     * 查询某序列在 [fromWindowStart, toWindowStart] 内各窗口的聚合快照。
     * 只返回有样本的窗口；返回的都是不可变对象，可在锁外安全使用。
     */
    public List<WindowAggregation> query(String name, Map<String, String> labels,
                                         long fromWindowStart, long toWindowStart) {
        SeriesKey key = SeriesKey.of(name, labels == null ? Map.of() : labels);
        List<WindowAggregation> out = new ArrayList<>();
        List<SeriesWindow> drained = new ArrayList<>();
        lock.lock();
        try {
            TreeMap<Long, SeriesWindow> windows = series.get(key);
            if (windows != null) {
                NavigableMap<Long, SeriesWindow> range =
                        windows.subMap(fromWindowStart, true, toWindowStart, true);
                for (SeriesWindow w : range.values()) {
                    drained.add(w);
                }
            }
        } finally {
            lock.unlock();
        }
        for (SeriesWindow w : drained) {
            WindowAggregation agg = w.snapshot();
            if (agg != null) {
                out.add(agg);
            }
        }
        return out;
    }

    /**
     * 分组查询：在 [fromWindowStart, toWindowStart] 各窗口内，把同一指标的多条序列按
     * groupBy 键取值合并成组。
     *
     * <p>锁内只做取不可变快照（按窗口收集、按序列键排序），锁外用
     * {@link com.github.h3yow.telemetrymetricsjava.metrics.group.GroupAggregationBuilder}
     * 做纯函数合并。四类容量越界（窗口数 / 组合数 / 组内序列数 / 返回行数）分别抛出
     * 可区分的 {@link com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitException}，
     * 不静默截断、不合并不同组合。</p>
     */
    public List<com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation>
            queryGrouped(String name, List<String> groupBy,
                         long fromWindowStart, long toWindowStart,
                         int maxCombinations, int maxSeriesPerGroup,
                         int maxResultRows, int maxQueryWindows) {
        long requestedWindows = (toWindowStart - fromWindowStart) / windowSizeMs + 1;
        if (requestedWindows > maxQueryWindows) {
            throw new com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitException(
                    com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitReason
                            .TOO_MANY_QUERY_WINDOWS,
                    maxQueryWindows, requestedWindows,
                    "grouped query spans " + requestedWindows + " windows, exceeds maxQueryWindows="
                            + maxQueryWindows);
        }

        // windowStart -> 该窗口内命中指标的各序列快照（锁内收集，锁外合并）。
        TreeMap<Long, List<WindowAggregation>> perWindow = new TreeMap<>();
        lock.lock();
        try {
            for (Map.Entry<SeriesKey, TreeMap<Long, SeriesWindow>> entry : series.entrySet()) {
                if (!entry.getKey().name().equals(name)) {
                    continue;
                }
                NavigableMap<Long, SeriesWindow> range =
                        entry.getValue().subMap(fromWindowStart, true, toWindowStart, true);
                for (SeriesWindow w : range.values()) {
                    WindowAggregation agg = w.snapshot();
                    if (agg != null) {
                        perWindow.computeIfAbsent(agg.windowStart(), k -> new ArrayList<>()).add(agg);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        List<com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation> out =
                new ArrayList<>();
        for (Map.Entry<Long, List<WindowAggregation>> e : perWindow.entrySet()) {
            // 排序保证 lastValue 时间戳并列时取值确定，且分组结果完全可复现。
            List<WindowAggregation> snapshots = e.getValue();
            snapshots.sort(java.util.Comparator.comparing(
                    a -> SeriesKey.of(a.name(), a.labels()).render()));
            out.addAll(com.github.h3yow.telemetrymetricsjava.metrics.group.GroupAggregationBuilder
                    .buildForWindow(name, groupBy, e.getKey(), windowSizeMs, snapshots,
                            maxCombinations, maxSeriesPerGroup));
        }

        if (out.size() > maxResultRows) {
            throw new com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitException(
                    com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitReason
                            .TOO_MANY_RESULT_ROWS,
                    maxResultRows, out.size(),
                    "grouped query returns " + out.size() + " rows, exceeds maxGroupedQueryRows="
                            + maxResultRows);
        }
        return out;
    }

    /** 当前处理时间对应的窗口起点。 */
    public long currentWindowStart() {        lock.lock();
        try {
            return floorWindowStart(clock.millis());
        } finally {
            lock.unlock();
        }
    }

    /** 当前仍被保留的最老窗口起点（诊断/测试用）。 */
    public long oldestRetainedWindowStart() {
        lock.lock();
        try {
            long oldest = Long.MAX_VALUE;
            for (TreeMap<Long, SeriesWindow> windows : series.values()) {
                if (!windows.isEmpty()) {
                    oldest = Math.min(oldest, windows.firstKey());
                }
            }
            return oldest == Long.MAX_VALUE ? -1L : oldest;
        } finally {
            lock.unlock();
        }
    }

    /** 当前活跃序列数（诊断/测试用）。 */
    public int activeSeriesCount() {
        lock.lock();
        try {
            return series.size();
        } finally {
            lock.unlock();
        }
    }

    /** 存活窗口总数（诊断/测试用）。 */
    public int retainedWindowCount() {
        lock.lock();
        try {
            int n = 0;
            for (TreeMap<Long, SeriesWindow> windows : series.values()) {
                n += windows.size();
            }
            return n;
        } finally {
            lock.unlock();
        }
    }

    long floorWindowStart(long timestamp) {
        return Math.floorDiv(timestamp, windowSizeMs) * windowSizeMs;
    }

    long windowSizeMs() {
        return windowSizeMs;
    }
}
