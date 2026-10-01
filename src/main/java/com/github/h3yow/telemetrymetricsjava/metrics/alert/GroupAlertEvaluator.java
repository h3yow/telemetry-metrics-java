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
import java.util.Iterator;
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
 * <p><b>恰好一次 / 乱序安全</b>：存储层对每个窗口只批量回调一次，并且跨线程严格按
 * 窗口起点升序投递（见 {@code MetricWindowStore} 的投递闸门）。本判定器在此之上再做
 * 两层保护：① 已应用窗口及重复回调幂等忽略（绝不重复判定）；② 即便窗口回调因任何
 * 原因乱序到达，只要调用方诚实告知封定水位，较晚到达的较老窗口会先被缓冲，状态机
 * 始终按事件时间窗口升序推进——较老窗口不会被整段跳过，状态与事件结果与顺序处理
 * 完全一致、可复现。</p>
 *
 * <p><b>容量边界</b>：组合数 / 组内序列数超限时该规则在该窗口的评估被跳过并计数、
 * 记录可解释信息（不影响其它规则），绝不静默合并或丢样本；事件队列有硬上界。</p>
 */
public class GroupAlertEvaluator {

    private static final int DEFAULT_MAX_EVENTS = 10_000;
    /** 诊断用越界明细的全局条数上界，保证长时间运行也不无界增长。 */
    private static final int MAX_REJECTION_ENTRIES = 1_000;

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
    /**
     * 已按序应用的最大窗口起点。窗口只可能应用一次：更早或相等的窗口回调一律幂等忽略
     * （存储层每窗口只投递一次，这里再做 O(1) 防御），杜绝乱序/重复回调造成的二次判定。
     */
    private long lastAppliedWindow = Long.MIN_VALUE;
    /**
     * 已到达但因更老窗口尚未送达而暂缓应用的窗口批次（按窗口起点升序）。
     * 只有当某窗口不超过“封定上界水位”时才真正应用状态机，从而无论回调到达顺序如何，
     * 状态推进与事件产出都严格按事件时间窗口顺序发生，结论与顺序处理时完全一致。
     * 存储层严格按窗口升序投递时，该缓冲恒为空（至多瞬时持有一个批次）。
     */
    private final TreeMap<Long, List<WindowAggregation>> pendingWindows = new TreeMap<>();
    private long sealWatermark = Long.MIN_VALUE;
    private final Deque<GroupAlertEvent> events = new ArrayDeque<>();
    private long droppedEvents;
    private long rejectedEvaluations;
    /** ruleId#windowStart -> 最近一次容量越界说明（诊断用，全局条数有界）。 */
    private final TreeMap<String, String> lastRejections = new TreeMap<>();

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
     * 等价于以 {@code windowStart} 自身作为封定水位调用三参版本：存储层已保证按窗口
     * 升序投递，因此该路径永远不会发生乱序缓冲。
     *
     * @return 本次（及因本次到达而得以按序补刷的更早窗口）产生的状态翻转事件；
     *         同一窗口不会被重复评估。
     */
    public List<GroupAlertEvent> evaluateWindow(long windowStart,
                                                List<WindowAggregation> sealedSeries) {
        return evaluateWindow(windowStart, sealedSeries, windowStart);
    }

    /**
     * 评估一个封定窗口，并显式告知本次封定周期的“封定上界水位”
     * （{@code sealHorizonWindowStart}：调用方已把所有有样本且起点 ≤ 该水位的窗口
     * 全部送达；更老的窗口若尚未出现，则永远不会再出现——它本来就没有样本）。
     *
     * <p>无论回调以什么顺序到达，判定都严格按<b>窗口起点升序</b>应用：
     * 若先到的窗口起点超过水位（说明更老的窗口还在途），它会被暂存等待；
     * 一旦更老窗口补齐或水位推进越过缺口，再按事件时间顺序补刷。
     * 因此滞回状态的推进、状态回收与事件产出，与“窗口按顺序处理”时完全一致、可复现。</p>
     *
     * <p>已经应用过的窗口（含其重复回调）以及更早窗口迟到的重投都会被幂等忽略，
     * 同一分组同一窗口<b>恰好判定一次</b>，不漏不重。</p>
     */
    public List<GroupAlertEvent> evaluateWindow(long windowStart,
                                                List<WindowAggregation> sealedSeries,
                                                long sealHorizonWindowStart) {
        if (sealedSeries == null || sealedSeries.isEmpty()) {
            // 空批次不携带窗口信息：存储层从不投递空批次，保持无副作用。
            return List.of();
        }
        long now = clock.millis();
        List<GroupAlertEvent> produced = new ArrayList<>();

        lock.lock();
        try {
            if (sealHorizonWindowStart > sealWatermark) {
                sealWatermark = sealHorizonWindowStart;
            }
            // 已应用过的窗口：重复回调或迟到重投，直接幂等丢弃。
            if (windowStart <= lastAppliedWindow) {
                return produced;
            }
            // 同一窗口在途重复回调：保留第一份（快照不可变，内容相同），不重复入队。
            pendingWindows.putIfAbsent(windowStart, List.copyOf(sealedSeries));

            // 只应用“水位以内”且已到齐的窗口升序前缀；超过水位的最小在途窗口必须等待
            // 更老窗口（可能仍在途中），从而保证状态机永远按事件时间推进。
            var ready = pendingWindows.headMap(sealWatermark, true);
            Iterator<Map.Entry<Long, List<WindowAggregation>>> it = ready.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, List<WindowAggregation>> e = it.next();
                long w = e.getKey();
                it.remove();
                applyWindow(w, e.getValue(), now, produced);
                lastAppliedWindow = w;
            }
        } finally {
            lock.unlock();
        }
        return produced;
    }

    /**
     * 仅推进封定水位而不携带新窗口数据：调用方完成了一次封定周期（上界为
     * {@code sealHorizonWindowStart}），该周期内没有任何有样本窗口产生批次。
     * 这会把“更老窗口只是缺样本、永远不会到达”的事实告知判定器，使在途缓冲中
     * 起点 ≤ 水位的窗口可以按序补刷。存储层在每次维护（含零批次）后调用。
     */
    public List<GroupAlertEvent> advanceSealWatermark(long sealHorizonWindowStart) {
        long now = clock.millis();
        List<GroupAlertEvent> produced = new ArrayList<>();
        lock.lock();
        try {
            if (sealHorizonWindowStart <= sealWatermark) {
                return produced;
            }
            sealWatermark = sealHorizonWindowStart;
            var ready = pendingWindows.headMap(sealWatermark, true);
            Iterator<Map.Entry<Long, List<WindowAggregation>>> it = ready.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, List<WindowAggregation>> e = it.next();
                long w = e.getKey();
                it.remove();
                applyWindow(w, e.getValue(), now, produced);
                lastAppliedWindow = w;
            }
        } finally {
            lock.unlock();
        }
        return produced;
    }

    /**
     * 按规则顺序把单个已就绪窗口应用到各分组的滞回状态机。
     * 调用保证同一窗口在整个生命周期内只进入本方法一次。
     */
    private void applyWindow(long windowStart, List<WindowAggregation> sealedSeries,
                             long now, List<GroupAlertEvent> produced) {
        for (GroupAlertRule rule : rules) {
            // 按事件时间顺序物理回收已过保留期的状态，保证再次出现时按新组
            // 从 RESOLVED 开始（同时为状态条目数提供上界）。
            expireStatuses(rule, windowStart);

            // 只挑本规则指标的序列（输入快照为不可变对象，计算量受容量上界约束）。
            List<WindowAggregation> ofMetric = new ArrayList<>();
            for (WindowAggregation agg : sealedSeries) {
                if (agg.name().equals(rule.metricName())) {
                    ofMetric.add(agg);
                }
            }
            if (ofMetric.isEmpty()) {
                // 本窗口该指标完全无样本：任何分组都不推进；保留期过期状态已在上面回收。
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
                while (lastRejections.size() > MAX_REJECTION_ENTRIES) {
                    String oldest = lastRejections.firstKey();
                    lastRejections.remove(oldest);
                }
                continue;
            }

            // 本窗口出现过的分组已在 evaluateGroup 中更新 lastSeenWindow。
            for (GroupWindowAggregation g : groups) {
                String groupKey = GroupAggregationBuilder.renderGroup(g.groupLabels());
                evaluateGroup(rule, g, groupKey, windowStart, now, produced);
            }
        }
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

    /**
     * 连续缺席超过保留期才过期：lastSeenWindow &lt; 当前评估窗口 - retention*size。
     * 恰好相隔 retention 个窗口长度（缺席了 retention 个窗口后于第 retention+1 个窗口
     * 回归）仍在保留期内、沿用原状态；缺席 &gt; retention 个窗口才回收。
     * 判定只依赖窗口起点之差，乱序/缓冲按序补刷时结论同样稳定。
     */
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
