package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import com.github.h3yow.telemetrymetricsjava.metrics.group.GroupAggregationBuilder;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupLimitException;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 分组告警判定器：消费“每个窗口恰好一次”的全量封定快照（同一窗口的全部序列），
 * 按各分组规则把多条序列聚合成组，再对每个分组独立做滞回判定。
 *
 * <p><b>缺样本窗口</b>：evaluateWindow 的入参只包含<b>有样本</b>的序列窗口，没有样本的
 * 分组根本不会出现在聚合结果里，因此不会被当成 0 值或正常值强行推进状态。</p>
 *
 * <p><b>消失再出现</b>：状态按「规则 × 分组」保留 groupRetentionWindows 个窗口。
 * 保留期内该分组没有样本时状态原样保留，重新出现沿用原状态（仍 FIRING 就不重复告警）；
 * 连续超过保留期无样本才淘汰状态，再次出现按新分组从 RESOLVED 开始。</p>
 *
 * <p><b>恰好一次</b>：存储层对每个窗口只批量回调一次，判定器还按
 * （规则, 窗口）做单调性保护，确保同一分组同一窗口只判定一次。</p>
 *
 * <p><b>容量边界</b>：组合数 / 组内序列数超限时该规则在该窗口的评估被跳过并计数、
 * 记录可解释信息（不影响其它规则），绝不静默合并或丢样本；事件队列有硬上界。</p>
 */
public class GroupAlertEvaluator {

    private static final int DEFAULT_MAX_EVENTS = 10_000;

    private final Map<String, GroupAlertRule> rulesById = new HashMap<>();
    private final List<GroupAlertRule> rules = new ArrayList<>();
    private final Clock clock;
    private final int maxGroupCombinations;
    private final int maxSeriesPerGroup;
    private final int groupRetentionWindows;
    private final long windowSizeMs;
    private final int maxEvents;

    private final ReentrantLock lock = new ReentrantLock();
    /** key = ruleId + '#' + 分组渲染串。 */
    private final Map<String, MutableStatus> statuses = new HashMap<>();
    /** ruleId -> 最近已判定窗口，单调保护，杜绝并发/重复回调造成的同窗二次判定。 */
    private final Map<String, Long> lastEvaluatedWindowByRule = new HashMap<>();
    private final Deque<GroupAlertEvent> events = new ArrayDeque<>();
    private long droppedEvents;
    private long rejectedEvaluations;
    /** ruleId#windowStart -> 最近一次容量越界说明（诊断用）。 */
    private final Map<String, String> lastRejections = new TreeMap<>();

    public GroupAlertEvaluator(List<GroupAlertRule> rules, Clock clock,
                               int maxGroupCombinations, int maxSeriesPerGroup,
                               int groupRetentionWindows, long windowSizeMs, int maxEvents) {
        if (maxGroupCombinations <= 0 || maxSeriesPerGroup <= 0 || maxEvents <= 0) {
            throw new IllegalArgumentException("capacity limits must be positive");
        }
        if (groupRetentionWindows < 0 || windowSizeMs <= 0) {
            throw new IllegalArgumentException("retention must be >= 0 and windowSizeMs positive");
        }
        this.clock = clock;
        this.maxGroupCombinations = maxGroupCombinations;
        this.maxSeriesPerGroup = maxSeriesPerGroup;
        this.groupRetentionWindows = groupRetentionWindows;
        this.windowSizeMs = windowSizeMs;
        this.maxEvents = maxEvents;
        for (GroupAlertRule rule : rules) {
            addRule(rule);
        }
    }

    public GroupAlertEvaluator(List<GroupAlertRule> rules, Clock clock,
                               int maxGroupCombinations, int maxSeriesPerGroup,
                               int groupRetentionWindows, long windowSizeMs) {
        this(rules, clock, maxGroupCombinations, maxSeriesPerGroup,
                groupRetentionWindows, windowSizeMs, DEFAULT_MAX_EVENTS);
    }

    /** 动态注册一条分组规则；重复 ruleId 被拒绝。 */
    public void addRule(GroupAlertRule rule) {
        lock.lock();
        try {
            if (rulesById.containsKey(rule.ruleId())) {
                throw new IllegalArgumentException("duplicate group ruleId: " + rule.ruleId());
            }
            rulesById.put(rule.ruleId(), rule);
            rules.add(rule);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 评估一个封定窗口。入参为该窗口封定的全部序列快照（仅含存在样本的序列窗口）。
     * 返回本次评估产生的状态翻转事件；同一（规则, 窗口）不会被重复评估。
     */
    public List<GroupAlertEvent> evaluateWindow(long windowStart,
                                                List<WindowAggregation> sealedSeries) {
        if (sealedSeries == null || sealedSeries.isEmpty()) {
            return List.of();
        }
        long now = clock.millis();
        List<GroupAlertEvent> produced = new ArrayList<>();

        lock.lock();
        try {
            for (GroupAlertRule rule : rules) {
                // 单调保护：窗口起点必须严格大于该规则上一次已判定的窗口。
                Long lastWindow = lastEvaluatedWindowByRule.get(rule.ruleId());
                if (lastWindow != null && windowStart <= lastWindow) {
                    continue;
                }

                // 无论本窗口是否有数据，都物理回收已过保留期的状态，保证再次出现时按新组
                // 从 RESOLVED 开始（同时为状态条目数提供上界）。
                expireStatuses(rule, windowStart);

                // 只挑本规则指标的序列，按序列键稳定排序后聚合（聚合在锁内，保证状态一致；
                // 输入快照为不可变对象，计算量受容量上界约束）。
                List<WindowAggregation> ofMetric = new ArrayList<>();
                for (WindowAggregation agg : sealedSeries) {
                    if (agg.name().equals(rule.metricName())) {
                        ofMetric.add(agg);
                    }
                }
                if (ofMetric.isEmpty()) {
                    // 本窗口该指标完全无样本：任何分组都不推进；保留期过期状态已在上面回收。
                    lastEvaluatedWindowByRule.put(rule.ruleId(), windowStart);
                    continue;
                }
                ofMetric.sort(Comparator.comparing(
                        a -> com.github.h3yow.telemetrymetricsjava.metrics.window.SeriesKey
                                .of(a.name(), a.labels()).render()));

                List<GroupWindowAggregation> groups;
                try {
                    groups = GroupAggregationBuilder.buildForWindow(rule.metricName(),
                            rule.groupBy(), windowStart, windowSizeMs, ofMetric,
                            maxGroupCombinations, maxSeriesPerGroup);
                } catch (GroupLimitException ex) {
                    // 容量越界：跳过本规则本窗口评估（不合并、不造数），记录可区分反馈。
                    rejectedEvaluations++;
                    lastRejections.put(rule.ruleId() + '#' + windowStart,
                            ex.reason() + " limit=" + ex.limit() + " actual=" + ex.actual()
                                    + " : " + ex.getMessage());
                    lastEvaluatedWindowByRule.put(rule.ruleId(), windowStart);
                    continue;
                }

                // 本窗口出现过的分组已在 evaluateGroup 中更新 lastSeenWindow。
                for (GroupWindowAggregation g : groups) {
                    String groupKey = GroupAggregationBuilder.renderGroup(g.groupLabels());
                    evaluateGroup(rule, g, groupKey, windowStart, now, produced);
                }

                lastEvaluatedWindowByRule.put(rule.ruleId(), windowStart);
            }
        } finally {
            lock.unlock();
        }
        return produced;
    }

    /** 对单个分组应用滞回状态机；只有状态真正翻转才产生事件。 */
    private void evaluateGroup(GroupAlertRule rule, GroupWindowAggregation g, String groupKey,
                               long windowStart, long now, List<GroupAlertEvent> produced) {
        String statusKey = rule.ruleId() + '#' + groupKey;
        MutableStatus status = statuses.computeIfAbsent(statusKey,
                k -> new MutableStatus(groupKey, g.groupLabels()));

        double observed = rule.extract(g);
        AlertState previous = status.state;
        AlertState next = previous;
        String decision;
        if (Double.isNaN(observed)) {
            decision = "group=" + groupKey + " observed=NaN -> state unchanged(" + previous + ")";
        } else if (previous == AlertState.RESOLVED && observed >= rule.fireThreshold()) {
            next = AlertState.FIRING;
            decision = "group=" + groupKey + " observed=" + observed
                    + " >= fireThreshold=" + rule.fireThreshold() + " -> FIRING";
        } else if (previous == AlertState.FIRING && observed <= rule.recoverThreshold()) {
            next = AlertState.RESOLVED;
            decision = "group=" + groupKey + " observed=" + observed
                    + " <= recoverThreshold=" + rule.recoverThreshold() + " -> RESOLVED";
        } else {
            decision = "group=" + groupKey + " observed=" + observed
                    + " inside hysteresis band (recover=" + rule.recoverThreshold()
                    + ", fire=" + rule.fireThreshold() + ") -> state unchanged(" + previous + ")";
        }

        status.lastObserved = observed;
        status.lastEvaluatedWindow = windowStart;
        status.lastSeenWindow = windowStart;
        if (next != previous) {
            status.state = next;
            status.stateChangedAtWindow = windowStart;
            GroupAlertEvent event = new GroupAlertEvent(rule.ruleId(),
                    Map.copyOf(new TreeMap<>(g.groupLabels())), groupKey, next,
                    windowStart, observed, g, decision, now);
            events.addLast(event);
            while (events.size() > maxEvents) {
                events.pollFirst();
                droppedEvents++;
            }
            produced.add(event);
        }
    }

    /**
     * 淘汰连续超过保留期没有样本的分组状态。保留期内（lastSeenWindow +
     * retention*size &gt;= 当前评估窗口）消失再出现沿用原状态，不算新触发。
     */
    private void expireStatuses(GroupAlertRule rule, long currentWindowStart) {
        String prefix = rule.ruleId() + '#';
        statuses.entrySet().removeIf(e ->
                e.getKey().startsWith(prefix) && isExpired(e.getValue(), currentWindowStart));
    }

    /** lastSeenWindow 早于 currentWindowStart - retention*size 即视为过期。 */
    private boolean isExpired(MutableStatus s, long currentWindowStart) {
        if (groupRetentionWindows == Integer.MAX_VALUE) {
            return false;
        }
        long horizon = currentWindowStart - (long) groupRetentionWindows * windowSizeMs;
        return s.lastSeenWindow < horizon;
    }

    /** 当前全部规则 × 分组的状态快照，按规则 ID、分组键稳定排序。 */
    public List<GroupAlertStatusView> currentStatuses() {
        lock.lock();
        try {
            // 判定只在有窗口封定时发生；若某指标整个窗口都没有样本，则不会有该窗的回调，
            // 因此这里再按当前时钟做一次非破坏性的过期过滤，保证状态视图即时反映保留期语义。
            long currentWindowStart = Math.floorDiv(clock.millis(), windowSizeMs) * windowSizeMs;
            List<GroupAlertStatusView> out = new ArrayList<>(statuses.size());
            for (Map.Entry<String, MutableStatus> e : statuses.entrySet()) {
                MutableStatus s = e.getValue();
                if (isExpired(s, currentWindowStart)) {
                    continue;
                }
                String ruleId = e.getKey().substring(0, e.getKey().indexOf('#'));
                out.add(new GroupAlertStatusView(ruleId,
                        Map.copyOf(new TreeMap<>(s.groupLabels)), s.groupKey, s.state,
                        s.lastEvaluatedWindow, s.lastObserved, s.stateChangedAtWindow));
            }
            out.sort(Comparator.comparing(GroupAlertStatusView::ruleId)
                    .thenComparing(GroupAlertStatusView::groupKey));
            return out;
        } finally {
            lock.unlock();
        }
    }

    /** 取走并清空已产生的分组告警翻转事件（按产生顺序）。 */
    public List<GroupAlertEvent> drainEvents() {
        lock.lock();
        try {
            List<GroupAlertEvent> out = new ArrayList<>(events);
            events.clear();
            return out;
        } finally {
            lock.unlock();
        }
    }

    public long droppedEventCount() {
        lock.lock();
        try {
            return droppedEvents;
        } finally {
            lock.unlock();
        }
    }

    public long rejectedEvaluationCount() {
        lock.lock();
        try {
            return rejectedEvaluations;
        } finally {
            lock.unlock();
        }
    }

    public Map<String, String> lastRejections() {
        lock.lock();
        try {
            return Map.copyOf(lastRejections);
        } finally {
            lock.unlock();
        }
    }

    /** 可变分组状态载体，仅在锁内访问。 */
    private static final class MutableStatus {
        final String groupKey;
        final Map<String, String> groupLabels;
        AlertState state = AlertState.RESOLVED;
        double lastObserved = Double.NaN;
        long lastEvaluatedWindow = -1L;
        long lastSeenWindow = -1L;
        long stateChangedAtWindow = -1L;

        MutableStatus(String groupKey, Map<String, String> groupLabels) {
            this.groupKey = groupKey;
            this.groupLabels = new TreeMap<>(groupLabels);
        }
    }
}
