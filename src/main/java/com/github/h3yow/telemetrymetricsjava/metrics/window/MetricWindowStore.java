package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryStatus;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
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
    private final int maxGroupCombinations;
    private final int maxSeriesPerGroup;
    private final int maxQueryWindows;
    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * seriesKey -> (windowStart -> 窗口累加器)。外层用 TreeMap（按 {@link SeriesKey}
     * 自然顺序）使分组遍历顺序与插入/哈希顺序无关，保证分组结果稳定可复现。
     */
    private final Map<SeriesKey, TreeMap<Long, SeriesWindow>> series = new TreeMap<>();

    /** labelKey -> 已出现过的 labelValue 集合，用于限制标签基数。 */
    private final Map<String, java.util.Set<String>> labelValues = new HashMap<>();

    private SealedWindowListener sealedWindowListener = aggregation -> { };
    private GroupedWindowSealedListener groupedWindowSealedListener =
            (metricName, windowStart, windowSizeMs, aggregations) -> { };
    /** 已做过“分组整体封定回调”的 metric#windowStart，保证每个（指标，窗口）恰好一次。 */
    private final java.util.Set<String> groupedFired = new java.util.HashSet<>();

    public MetricWindowStore(long windowSizeMs, int retainedWindows, int maxSamplesPerSeriesWindow,
                             int maxSeries, int maxLabelValuesPerKey, long maxFutureSkewMs,
                             long lateArrivalGraceMs, Clock clock) {
        this(windowSizeMs, retainedWindows, maxSamplesPerSeriesWindow, maxSeries,
                maxLabelValuesPerKey, maxFutureSkewMs, lateArrivalGraceMs,
                1_000, 5_000, 1_000, clock);
    }

    public MetricWindowStore(long windowSizeMs, int retainedWindows, int maxSamplesPerSeriesWindow,
                             int maxSeries, int maxLabelValuesPerKey, long maxFutureSkewMs,
                             long lateArrivalGraceMs,
                             int maxGroupCombinations, int maxSeriesPerGroup, int maxQueryWindows,
                             Clock clock) {
        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("windowSizeMs must be positive");
        }
        if (retainedWindows <= 0) {
            throw new IllegalArgumentException("retainedWindows must be positive");
        }
        if (maxSamplesPerSeriesWindow <= 0 || maxSeries <= 0 || maxLabelValuesPerKey <= 0) {
            throw new IllegalArgumentException("capacity limits must be positive");
        }
        if (maxGroupCombinations <= 0 || maxSeriesPerGroup <= 0 || maxQueryWindows <= 0) {
            throw new IllegalArgumentException("group/query capacity limits must be positive");
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
        this.maxGroupCombinations = maxGroupCombinations;
        this.maxSeriesPerGroup = maxSeriesPerGroup;
        this.maxQueryWindows = maxQueryWindows;
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

    /** 注册分组窗口封定回调（每个 metric×窗口恰好一次，交出该窗口全部序列快照）。 */
    public void setGroupedWindowSealedListener(GroupedWindowSealedListener listener) {
        lock.lock();
        try {
            this.groupedWindowSealedListener =
                    listener == null ? (m, w, s, a) -> { } : listener;
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
        }
    }

    private void maintainWindowsLocked(long now) {
        // 窗口 [s, s+size) 封定条件：s + size + grace <= now，故封定上界为
        // floor((now - grace - size) / size) * size。grace=0 时恰好在下一个窗口开始时封定。
        long sealHorizon = floorWindowStart(now - lateArrivalGraceMs - windowSizeMs);
        long oldestAllowed = floorWindowStart(now) - (long) (retainedWindows - 1) * windowSizeMs;

        Iterator<Map.Entry<SeriesKey, TreeMap<Long, SeriesWindow>>> it = series.entrySet().iterator();
        // 收集本次新封定窗口：(metric#windowStart) -> 有序序列快照列表。
        // 外层 series 是 TreeMap（按 SeriesKey 排序），故收集结果按序列键确定有序。
        Map<String, List<WindowAggregation>> newlySealed = new java.util.TreeMap<>();
        while (it.hasNext()) {
            Map.Entry<SeriesKey, TreeMap<Long, SeriesWindow>> entry = it.next();
            TreeMap<Long, SeriesWindow> windows = entry.getValue();

            // 1) 封定：窗口结束时间 + 宽限已过，且本窗口尚未封定 -> 回调一次聚合快照。
            for (SeriesWindow w : windows.headMap(sealHorizon, true).values()) {
                if (!w.isSealed()) {
                    w.markSealed();
                    WindowAggregation agg = w.snapshot();
                    if (agg != null) {
                        // 监听器只读取快照、不重入存储；回调在锁内保证“每窗口恰好一次”。
                        sealedWindowListener.onWindowSealed(agg);
                        newlySealed
                                .computeIfAbsent(agg.name() + '#' + agg.windowStart(),
                                        k -> new ArrayList<>())
                                .add(agg);
                    }
                }
            }

            // 2) 淘汰：早于保留下界的窗口直接移除（聚合已在封定时被告警侧消费）。
            windows.headMap(oldestAllowed, false).clear();
            if (windows.isEmpty()) {
                it.remove();
            }
        }

        // 3) 分组整体封定：newlySealed 是在外层“全部序列”循环结束后才统一处理的，
        //    因此它包含同一（指标，窗口）在本次 pass 中封定的全部序列——不会在只见到
        //    第一条序列时就提前回调。越过封定线的窗口此后不可能再收到样本
        //    （迟到者会被 TOO_OLD 拒绝），故每个（指标，窗口）在此完整且恰好回调一次。
        for (Map.Entry<String, List<WindowAggregation>> e : newlySealed.entrySet()) {
            String composite = e.getKey();
            if (groupedFired.add(composite)) {
                List<WindowAggregation> aggs = e.getValue(); // 已按序列键升序
                groupedWindowSealedListener.onGroupedWindowSealed(
                        aggs.get(0).name(), aggs.get(0).windowStart(), windowSizeMs,
                        List.copyOf(aggs));
            }
        }
        // 已淘汰窗口的封定记账也清掉，使记账集合大小有界（不随运行时间增长）。
        long sealEvictHorizon = oldestAllowed;
        groupedFired.removeIf(token -> {
            int hashIdx = token.lastIndexOf('#');
            long ws = Long.parseLong(token.substring(hashIdx + 1));
            return ws < sealEvictHorizon;
        });
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
     * 分组查询：把 [fromWindowStart, toWindowStart] 内、指标名相同的各序列各窗口快照，
     * 按 groupByKeys 投影分组合并。只返回有样本的（分组，窗口）；结果有序且确定。
     *
     * <p>窗口数超 {@code maxQueryWindows}、组合数超 {@code maxGroupCombinations}、
     * 组内序列数超 {@code maxSeriesPerGroup} 时整单失败并返回可区分状态，
     * 不静默截断、不跨组合合并。</p>
     */
    public GroupQueryResult queryGroups(String name, List<String> groupByKeys,
                                        long fromWindowStart, long toWindowStart) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("metric name must be present");
        }
        if (fromWindowStart > toWindowStart) {
            throw new IllegalArgumentException("fromWindowStart must be <= toWindowStart");
        }
        // groupByKeys 先规范化（去重、排序），结果与传入顺序无关。
        List<String> keys = groupByKeys == null
                ? List.of() : List.copyOf(new java.util.TreeSet<>(groupByKeys));

        long requestedWindows = (toWindowStart - fromWindowStart) / windowSizeMs + 1;
        if (requestedWindows > maxQueryWindows) {
            return GroupQueryResult.rejected(GroupQueryStatus.TOO_MANY_WINDOWS,
                    "requested window count=" + requestedWindows + " exceeds maxQueryWindows="
                            + maxQueryWindows);
        }

        // 锁内只做有界收集：外层 TreeMap 已按序列键排序，内层按窗口起点排序，
        // 因此取出的快照天然“先序列、后窗口”有序，结果不依赖哈希迭代顺序。
        List<WindowAggregation> snapshots = new ArrayList<>();
        lock.lock();
        try {
            for (Map.Entry<SeriesKey, TreeMap<Long, SeriesWindow>> e : series.entrySet()) {
                if (!e.getKey().name().equals(name)) {
                    continue;
                }
                NavigableMap<Long, SeriesWindow> range =
                        e.getValue().subMap(fromWindowStart, true, toWindowStart, true);
                for (SeriesWindow w : range.values()) {
                    WindowAggregation agg = w.snapshot();
                    if (agg != null) {
                        snapshots.add(agg);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        // 按窗口起点分桶（TreeMap 保证窗口升序）；桶内保持序列键升序。
        TreeMap<Long, List<WindowAggregation>> byWindow = new TreeMap<>();
        for (WindowAggregation agg : snapshots) {
            byWindow.computeIfAbsent(agg.windowStart(), k -> new ArrayList<>()).add(agg);
        }

        List<GroupWindowAggregation> out = new ArrayList<>();
        for (Map.Entry<Long, List<WindowAggregation>> e : byWindow.entrySet()) {
            long windowStart = e.getKey();
            Map<GroupKey, List<WindowAggregation>> groups =
                    GroupWindowAggregator.partition(e.getValue(), keys);

            if (groups.size() > maxGroupCombinations) {
                return GroupQueryResult.rejected(GroupQueryStatus.TOO_MANY_GROUP_COMBINATIONS,
                        "group combinations=" + groups.size() + " in window " + windowStart
                                + " exceeds maxGroupCombinations=" + maxGroupCombinations);
            }
            for (Map.Entry<GroupKey, List<WindowAggregation>> g : groups.entrySet()) {
                if (g.getValue().size() > maxSeriesPerGroup) {
                    return GroupQueryResult.rejected(GroupQueryStatus.TOO_MANY_SERIES_IN_GROUP,
                            "group " + g.getKey().render() + " in window " + windowStart
                                    + " contains " + g.getValue().size()
                                    + " series, exceeds maxSeriesPerGroup=" + maxSeriesPerGroup);
                }
            }
            for (Map.Entry<GroupKey, List<WindowAggregation>> g : groups.entrySet()) {
                out.add(GroupWindowAggregator.merge(g.getKey(), windowStart, windowSizeMs,
                        g.getValue()));
            }
        }
        return GroupQueryResult.ok(out);
    }

    int maxGroupCombinations() {
        return maxGroupCombinations;
    }

    int maxSeriesPerGroup() {
        return maxSeriesPerGroup;
    }

    int maxQueryWindows() {
        return maxQueryWindows;
    }

    /** 当前处理时间对应的窗口起点。 */
    public long currentWindowStart() {
        lock.lock();
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
