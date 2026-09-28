package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.window.GroupKey;
import com.github.h3yow.telemetrymetricsjava.metrics.window.GroupWindowAggregator;
import com.github.h3yow.telemetrymetricsjava.metrics.window.SeriesKey;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 告警判定器：消费“每个序列每个窗口恰好一次”的封定聚合快照，维护跨窗口的滞回状态。
 *
 * <p><b>状态机</b>：RESOLVED --observed &gt;= fire--&gt; FIRING；
 * FIRING --observed &lt;= recover--&gt; RESOLVED；两阈值之间为死区，维持现状。
 * 状态不翻转就不产生事件，因此阈值附近抖动不会重复告警；恢复之后再次越线会重新触发。</p>
 *
 * <p><b>上界</b>：状态条目数 ≤ 规则数 × 序列数（序列数受存储层 maxSeries 约束）；
 * 事件队列长度有硬上界，超出时丢弃最旧事件并计数，绝不无界增长。</p>
 */
public class AlertEvaluator {

    private static final int DEFAULT_MAX_EVENTS = 10_000;

    private final List<AlertRule> rules = new ArrayList<>();
    private final Map<String, AlertRule> rulesById = new HashMap<>();
    private final Clock clock;
    private final int maxEvents;
    private final long windowSizeMs;
    private final int groupRetentionWindows;
    private final int maxGroupCombinations;
    private final int maxSeriesPerGroup;

    private final ReentrantLock lock = new ReentrantLock();
    /** key = ruleId + '#' + 序列渲染名。 */
    private final Map<String, MutableStatus> statuses = new HashMap<>();
    /** 分组规则状态：key = 规则 × 分组键。 */
    private final Map<GroupStatusKey, GroupMutableStatus> groupStatuses = new HashMap<>();
    private final Deque<AlertEvent> events = new ArrayDeque<>();
    private long droppedEvents;
    /** 分组判定时触发容量上界而跳过评估的（规则×窗口）次数，可区分、可观测。 */
    private long groupEvaluationSkips;
    private String lastGroupSkipDetail = "";

    public AlertEvaluator(List<AlertRule> rules) {
        this(rules, Clock.systemUTC(), DEFAULT_MAX_EVENTS);
    }

    public AlertEvaluator(List<AlertRule> rules, Clock clock, int maxEvents) {
        this(rules, clock, maxEvents, 10_000L, 6, 1_000, 5_000);
    }

    /**
     * @param windowSizeMs           窗口长度，用于把“保留窗口数”换算成状态淘汰时间跨度
     * @param groupRetentionWindows  分组消失后状态保留的窗口数
     * @param maxGroupCombinations   单次分组判定允许的分组组合数上限
     * @param maxSeriesPerGroup      单个分组允许合并的序列数上限
     */
    public AlertEvaluator(List<AlertRule> rules, Clock clock, int maxEvents, long windowSizeMs,
                          int groupRetentionWindows, int maxGroupCombinations,
                          int maxSeriesPerGroup) {
        if (groupRetentionWindows < 0 || maxGroupCombinations <= 0 || maxSeriesPerGroup <= 0) {
            throw new IllegalArgumentException("group alert limits must be valid");
        }
        this.clock = clock;
        this.maxEvents = maxEvents;
        this.windowSizeMs = windowSizeMs;
        this.groupRetentionWindows = groupRetentionWindows;
        this.maxGroupCombinations = maxGroupCombinations;
        this.maxSeriesPerGroup = maxSeriesPerGroup;
        for (AlertRule rule : rules) {
            addRule(rule);
        }
    }

    /**
     * 动态注册一条规则。重复 ruleId 会被拒绝。
     */
    public void addRule(AlertRule rule) {
        lock.lock();
        try {
            if (rulesById.containsKey(rule.ruleId())) {
                throw new IllegalArgumentException("duplicate ruleId: " + rule.ruleId());
            }
            rulesById.put(rule.ruleId(), rule);
            rules.add(rule);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 评估一个封定窗口的聚合（由存储层封定回调驱动，每窗口仅一次）。
     * 返回本次评估产生的状态翻转事件（可能为空）。
     */
    public List<AlertEvent> evaluate(WindowAggregation agg, long windowStart) {
        if (agg == null) {
            return List.of();
        }
        List<AlertEvent> produced = new ArrayList<>();
        long now = clock.millis();
        String seriesRendered = SeriesKey.of(agg.name(), agg.labels()).render();

        lock.lock();
        try {
            for (AlertRule rule : rules) {
                if (rule.grouped()) {
                    // 分组规则由 evaluateGroupedWindow 在窗口粒度统一判定，这里不按单序列触发。
                    continue;
                }
                if (!rule.matches(agg.name(), agg.labels())) {
                    continue;
                }
                double observed = rule.extract(agg);
                String statusKey = rule.ruleId() + '#' + seriesRendered;
                MutableStatus status =
                        statuses.computeIfAbsent(statusKey, k -> new MutableStatus(seriesRendered));

                AlertState previous = status.state;
                AlertState next = previous;
                String decision;
                if (Double.isNaN(observed)) {
                    decision = "observed=NaN -> state unchanged(" + previous + ")";
                } else if (previous == AlertState.RESOLVED && observed >= rule.fireThreshold()) {
                    next = AlertState.FIRING;
                    decision = "observed=" + observed + " >= fireThreshold="
                            + rule.fireThreshold() + " -> FIRING";
                } else if (previous == AlertState.FIRING && observed <= rule.recoverThreshold()) {
                    next = AlertState.RESOLVED;
                    decision = "observed=" + observed + " <= recoverThreshold="
                            + rule.recoverThreshold() + " -> RESOLVED";
                } else {
                    decision = "observed=" + observed + " inside hysteresis band (recover="
                            + rule.recoverThreshold() + ", fire=" + rule.fireThreshold()
                            + ") -> state unchanged(" + previous + ")";
                }

                status.lastObserved = observed;
                status.lastEvaluatedWindow = windowStart;
                if (next != previous) {
                    status.state = next;
                    status.stateChangedAtWindow = windowStart;
                    AlertEvent event = new AlertEvent(rule.ruleId(), seriesRendered, next,
                            windowStart, observed, decision, now);
                    events.addLast(event);
                    while (events.size() > maxEvents) {
                        events.pollFirst();
                        droppedEvents++;
                    }
                    produced.add(event);
                }
            }
        } finally {
            lock.unlock();
        }
        return produced;
    }

    /** 当前全部规则/序列的状态快照。 */
    public List<AlertStatusView> currentStatuses() {
        lock.lock();
        try {
            List<AlertStatusView> out = new ArrayList<>(statuses.size());
            for (Map.Entry<String, MutableStatus> e : statuses.entrySet()) {
                String ruleId = e.getKey().substring(0, e.getKey().indexOf('#'));
                MutableStatus s = e.getValue();
                out.add(new AlertStatusView(ruleId, s.seriesKey, s.state,
                        s.lastEvaluatedWindow, s.lastObserved, s.stateChangedAtWindow));
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** 取走并清空已产生的状态翻转事件。 */
    public List<AlertEvent> drainEvents() {
        lock.lock();
        try {
            List<AlertEvent> out = new ArrayList<>(events);
            events.clear();
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** 因事件队列达上界而被丢弃（覆盖）的旧事件数，诊断用。 */
    public long droppedEventCount() {
        lock.lock();
        try {
            return droppedEvents;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 分组窗口判定：消费一个（指标，窗口）下全部已封定序列快照，对每条<b>分组规则</b>
     * 按其 groupByKeys 投影分组，逐组做滞回判定。每个（规则，分组，窗口）只调用一次
     * （由存储层“每 metric×窗口恰好一次”回调保证）。
     *
     * <p>无样本的窗口根本不会进入本方法（存储层只回调有样本的窗口），因此缺窗口
     * 不会被当成 0 值/正常值推进状态。{@code windowAggregations} 为按序列键排序的
     * 不可变快照，调用在存储锁内、但本方法只使用入参，不触碰存储。</p>
     *
     * @return 本次产生的状态翻转事件（可能为空）
     */
    public List<AlertEvent> evaluateGroupedWindow(String metricName, long windowStart,
                                                  List<WindowAggregation> windowAggregations) {
        if (windowAggregations == null || windowAggregations.isEmpty()) {
            return List.of();
        }
        List<AlertEvent> produced = new ArrayList<>();
        long now = clock.millis();

        lock.lock();
        try {
            for (AlertRule rule : rules) {
                if (!rule.grouped() || !rule.metricName().equals(metricName)) {
                    continue;
                }
                // 只让通过标签匹配的序列参与该规则的分组。
                List<WindowAggregation> matched = new ArrayList<>();
                for (WindowAggregation agg : windowAggregations) {
                    if (rule.matches(agg.name(), agg.labels())) {
                        matched.add(agg);
                    }
                }
                if (matched.isEmpty()) {
                    // 该窗口下没有任何序列属于本规则：不推进状态（缺窗口/无样本不等于 0 值）。
                    evictStaleGroupStatusesLocked(rule, windowStart, java.util.Set.of());
                    continue;
                }

                Map<GroupKey, List<WindowAggregation>> buckets =
                        GroupWindowAggregator.partition(matched, rule.groupByKeys());
                if (buckets.size() > maxGroupCombinations) {
                    groupEvaluationSkips++;
                    lastGroupSkipDetail = "rule=" + rule.ruleId() + " window=" + windowStart
                            + " groupCombinations=" + buckets.size()
                            + " exceeds maxGroupCombinations=" + maxGroupCombinations
                            + "; evaluation skipped for this rule-window";
                    continue;
                }
                boolean overSeries = false;
                for (var e : buckets.entrySet()) {
                    if (e.getValue().size() > maxSeriesPerGroup) {
                        groupEvaluationSkips++;
                        lastGroupSkipDetail = "rule=" + rule.ruleId() + " window=" + windowStart
                                + " group=" + e.getKey().render() + " series=" + e.getValue().size()
                                + " exceeds maxSeriesPerGroup=" + maxSeriesPerGroup
                                + "; evaluation skipped for this rule-window";
                        overSeries = true;
                        break;
                    }
                }
                if (overSeries) {
                    continue;
                }

                java.util.Set<GroupKey> seenThisWindow = new java.util.HashSet<>();
                for (Map.Entry<GroupKey, List<WindowAggregation>> e : buckets.entrySet()) {
                    GroupKey groupKey = e.getKey();
                    seenThisWindow.add(groupKey);
                    GroupWindowAggregation gav = GroupWindowAggregator.merge(
                            groupKey, windowStart, windowAggregations.get(0).windowSize(),
                            e.getValue());
                    AlertEvent event = evaluateOneGroupLocked(rule, groupKey, gav, windowStart, now);
                    if (event != null) {
                        produced.add(event);
                    }
                }
                evictStaleGroupStatusesLocked(rule, windowStart, seenThisWindow);
            }
        } finally {
            lock.unlock();
        }
        return produced;
    }

    /**
     * 单个（规则，分组，窗口）的滞回判定。调用方持锁。
     * 没有样本的分组不会进入这里；分组在窗口中缺席时本方法不被调用，状态原样保留。
     */
    private AlertEvent evaluateOneGroupLocked(AlertRule rule, GroupKey groupKey,
                                              GroupWindowAggregation gav,
                                              long windowStart, long now) {
        GroupStatusKey key = new GroupStatusKey(rule.ruleId(), groupKey);
        GroupMutableStatus status = groupStatuses.computeIfAbsent(
                key, k -> new GroupMutableStatus(groupKey));

        double observed = rule.extractGroup(gav);
        AlertState previous = status.state;
        AlertState next = previous;
        String basis = "group=" + groupKey.render()
                + " samples=" + gav.sampleCount() + " series=" + gav.seriesCount()
                + " window=" + windowStart + " observed(" + rule.aggregator() + ")=" + observed;
        String decision;
        if (Double.isNaN(observed)) {
            decision = basis + " is NaN -> state unchanged(" + previous + ")";
        } else if (previous == AlertState.RESOLVED && observed >= rule.fireThreshold()) {
            next = AlertState.FIRING;
            decision = basis + " >= fireThreshold=" + rule.fireThreshold() + " -> FIRING";
        } else if (previous == AlertState.FIRING && observed <= rule.recoverThreshold()) {
            next = AlertState.RESOLVED;
            decision = basis + " <= recoverThreshold=" + rule.recoverThreshold() + " -> RESOLVED";
        } else {
            decision = basis + " inside hysteresis band (recover=" + rule.recoverThreshold()
                    + ", fire=" + rule.fireThreshold() + ") -> state unchanged(" + previous + ")";
        }

        status.lastObserved = observed;
        status.lastEvaluatedWindow = windowStart;
        status.lastSampleCount = gav.sampleCount();
        status.lastSeriesCount = gav.seriesCount();
        if (next != previous) {
            status.state = next;
            status.stateChangedAtWindow = windowStart;
            String rendered = rule.metricName() + groupKey.render();
            AlertEvent event = new AlertEvent(rule.ruleId(), rendered, next, windowStart,
                    observed, decision, now, true, groupKey.labels(),
                    gav.seriesCount(), gav.sampleCount());
            enqueueLocked(event);
            return event;
        }
        return null;
    }

    /**
     * 淘汰长时间未出现的分组状态：自最近判定窗口起超过
     * {@code groupRetentionWindows} 个窗口未再出现的分组，静默丢弃其状态条目，
     * 使状态表不随标签组合数无界增长。明确保留期内的缺席保留原状态，
     * 回来时不算新触发；超过保留期后回来按新的 RESOLVED 组处理（可能重新触发）。
     * 淘汰不补发 RESOLVED 事件（分组已不存在，无可通知对象）。
     */
    private void evictStaleGroupStatusesLocked(AlertRule rule, long currentWindow,
                                               java.util.Set<GroupKey> presentThisWindow) {
        long staleBefore = currentWindow
                - (long) groupRetentionWindows * windowSizeMs;
        groupStatuses.entrySet().removeIf(en ->
                en.getKey().ruleId().equals(rule.ruleId())
                        && !presentThisWindow.contains(en.getKey().groupKey())
                        && en.getValue().lastEvaluatedWindow < staleBefore);
    }

    /** 事件入队并在超界时丢弃最旧事件、计数。调用方持锁。 */
    private void enqueueLocked(AlertEvent event) {
        events.addLast(event);
        while (events.size() > maxEvents) {
            events.pollFirst();
            droppedEvents++;
        }
    }

    /** 分组规则当前各分组的状态快照。 */
    public List<AlertStatusView> currentGroupStatuses() {
        lock.lock();
        try {
            List<AlertStatusView> out = new ArrayList<>(groupStatuses.size());
            for (Map.Entry<GroupStatusKey, GroupMutableStatus> e : groupStatuses.entrySet()) {
                GroupMutableStatus s = e.getValue();
                AlertRule rule = rulesById.get(e.getKey().ruleId());
                String rendered = (rule == null ? e.getKey().ruleId() : rule.metricName())
                        + s.groupKey.render();
                out.add(new AlertStatusView(e.getKey().ruleId(), rendered, s.state,
                        s.lastEvaluatedWindow, s.lastObserved, s.stateChangedAtWindow,
                        true, s.groupKey.labels()));
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** 分组判定因容量上界被跳过的次数（诊断用）。 */
    public long groupEvaluationSkipCount() {
        lock.lock();
        try {
            return groupEvaluationSkips;
        } finally {
            lock.unlock();
        }
    }

    /** 最近一次分组判定跳过的可解释说明（诊断用）。 */
    public String lastGroupSkipDetail() {
        lock.lock();
        try {
            return lastGroupSkipDetail;
        } finally {
            lock.unlock();
        }
    }

    /** 分组状态键：规则 × 分组身份。 */
    private record GroupStatusKey(String ruleId, GroupKey groupKey) {
    }

    /** 分组可变状态载体，仅在锁内访问。 */
    private static final class GroupMutableStatus {
        final GroupKey groupKey;
        AlertState state = AlertState.RESOLVED;
        double lastObserved = Double.NaN;
        long lastEvaluatedWindow = -1L;
        long stateChangedAtWindow = -1L;
        long lastSampleCount;
        long lastSeriesCount;

        GroupMutableStatus(GroupKey groupKey) {
            this.groupKey = groupKey;
        }
    }

    /** 可变状态载体，仅在锁内访问。 */
    private static final class MutableStatus {
        final String seriesKey;
        AlertState state = AlertState.RESOLVED;
        double lastObserved = Double.NaN;
        long lastEvaluatedWindow = -1L;
        long stateChangedAtWindow = -1L;

        MutableStatus(String seriesKey) {
            this.seriesKey = seriesKey;
        }
    }
}
