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
 * <p><b>并发与乱序下的恰好一次 / 结果一致</b>：存储层保证按窗口起点升序、每窗口一次地
 * 串行投递批次；本判定器在此之上再做两层防御，使得“不同窗口的判定先后被打乱”也绝不
 * 改变最终状态与事件：</p>
 * <ul>
 *   <li><b>去重</b>：以全局已应用窗口高水位 {@code appliedHighWatermark} 去重，
 *       窗口起点不大于高水位的重复投递直接忽略（返回空、不产生事件），同一（规则 ×
 *       分组 × 窗口）只可能被判定一次。</li>
 *   <li><b>乱序重排</b>：早于“下一个应处理窗口”的批次先进入有界重排缓冲
 *       {@code pendingWindows}（按窗口起点排序）；只要连续前缀齐备，就严格按窗口起点
 *       升序逐个应用。先到较晚窗口、后到较早窗口时，较早窗口不会被整段跳过，最终
 *       状态与事件顺序完全等价于按序处理，可复现。</li>
 * </ul>
 *
 * <p><b>缺样本窗口</b>：存储层会为没有任何样本的窗口投递<b>空批次</b>，使窗口序列
 * 连续无空洞。空批次只推进窗口序号（让后续乱序到达的数据窗口不必永久等待），
 * <b>不聚合、不过期、不推进任何分组状态</b>——绝不被当成 0 值或正常值，FIRING 分组
 * 不会因为缺窗口被“自动恢复”，保留期也只按有样本窗口计。</p>
 *
 * <p><b>消失再出现</b>：状态按「规则 × 分组」保留 groupRetentionWindows 个窗口。
 * 保留期内该分组没有样本时状态原样保留，重新出现沿用原状态（仍 FIRING 就不重复告警）；
 * 连续超过保留期无样本才淘汰状态，再次出现按新分组从 RESOLVED 开始。空窗口补位保证
 * 该结论与窗口到达/处理顺序无关。</p>
 *
 * <p><b>容量边界</b>：组合数 / 组内序列数超限时该规则在该窗口的评估被跳过并计数、
 * 记录可解释信息（不影响其它规则），绝不静默合并或丢样本；事件队列有硬上界。</p>
 */
public class GroupAlertEvaluator {

    private static final int DEFAULT_MAX_EVENTS = 10_000;
    /** 乱序重排缓冲的窗口数上界（防御性配置；有序投递的正常路径缓冲恒为空）。 */
    private static final int DEFAULT_MAX_PENDING_WINDOWS = 1_024;

    private final Map<String, GroupAlertRule> rulesById = new HashMap<>();
    private final List<GroupAlertRule> rules = new ArrayList<>();
    private final Clock clock;
    private final int maxGroupCombinations;
    private final int maxSeriesPerGroup;
    private final int groupRetentionWindows;
    private final long windowSizeMs;
    private final int maxEvents;
    private final int maxPendingWindows;

    private final ReentrantLock lock = new ReentrantLock();
    /** key = ruleId + '#' + 分组渲染串。 */
    private final Map<String, MutableStatus> statuses = new HashMap<>();
    /**
     * 已按序应用过的最大窗口起点；{@code null} 表示序列尚未锚定（还不知道最早会出现
     * 哪个窗口）。锚定后用于全局去重，杜绝重复回调造成的同窗二次判定。
     */
    private Long appliedHighWatermark = null;
    /**
     * 已收到但因更早窗口尚未到齐而暂存的批次（windowStart -> 快照列表，可能为空批次）。
     * 连续前缀齐备时立即升序排空；正常有序投递下该缓冲恒为空。
     */
    private final TreeMap<Long, List<WindowAggregation>> pendingWindows = new TreeMap<>();
    private final Deque<GroupAlertEvent> events = new ArrayDeque<>();
    private long droppedEvents;
    private long rejectedEvaluations;
    /** 因长期空洞超过重排缓冲上界而被迫提前放行的窗口数（诊断用；正常路径恒为 0）。 */
    private long forcedFlushedWindows;
    /** ruleId#windowStart -> 最近一次容量越界说明（诊断用）。 */
    private final Map<String, String> lastRejections = new TreeMap<>();

    public GroupAlertEvaluator(List<GroupAlertRule> rules, Clock clock,
                               int maxGroupCombinations, int maxSeriesPerGroup,
                               int groupRetentionWindows, long windowSizeMs, int maxEvents) {
        this(rules, clock, maxGroupCombinations, maxSeriesPerGroup,
                groupRetentionWindows, windowSizeMs, maxEvents, DEFAULT_MAX_PENDING_WINDOWS);
    }

    public GroupAlertEvaluator(List<GroupAlertRule> rules, Clock clock,
                               int maxGroupCombinations, int maxSeriesPerGroup,
                               int groupRetentionWindows, long windowSizeMs, int maxEvents,
                               int maxPendingWindows) {
        if (maxGroupCombinations <= 0 || maxSeriesPerGroup <= 0 || maxEvents <= 0) {
            throw new IllegalArgumentException("capacity limits must be positive");
        }
        if (groupRetentionWindows < 0 || windowSizeMs <= 0) {
            throw new IllegalArgumentException("retention must be >= 0 and windowSizeMs positive");
        }
        if (maxPendingWindows <= 0) {
            throw new IllegalArgumentException("maxPendingWindows must be positive");
        }
        this.clock = clock;
        this.maxGroupCombinations = maxGroupCombinations;
        this.maxSeriesPerGroup = maxSeriesPerGroup;
        this.groupRetentionWindows = groupRetentionWindows;
        this.windowSizeMs = windowSizeMs;
        this.maxEvents = maxEvents;
        this.maxPendingWindows = maxPendingWindows;
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
     * 声明窗口序列的最早窗口起点（<b>纯声明，不判定该窗口</b>）。乱序处理需要知道
     * “最早还会到来的窗口”才能判断连续前缀是否齐备：调用后高水位被置于
     * {@code firstWindowStart - windowSizeMs}，随后即使较晚窗口先到也会缓冲，等
     * {@code firstWindowStart} 及其后继到齐后再严格升序应用。
     *
     * <p>存储层总是从最早封定窗口开始升序投递，因此其首个 {@link #evaluateWindow}
     * 会自动完成锚定，无需调用本方法；直接使用判定器（如测试）且要先投递较晚窗口时，
     * 用本方法声明起点，最早窗口本身再通过 {@link #evaluateWindow} 正常投递。重复或
     * 晚于已应用水位的锚定被忽略。</p>
     */
    public void anchor(long firstWindowStart) {
        lock.lock();
        try {
            long anchorWatermark = firstWindowStart - windowSizeMs;
            if (appliedHighWatermark == null || anchorWatermark > appliedHighWatermark) {
                appliedHighWatermark = anchorWatermark;
                flushContiguous(new ArrayList<>());
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 交付一个封定窗口的全量序列快照。空列表表示该窗口全库无样本（缺窗口补位），
     * 只推进窗口序号，不推进任何分组状态、不做保留期回收。
     *
     * <p>本方法对调用顺序不敏感：重复（窗口起点不大于已应用高水位）投递被幂等忽略；
     * 乱序（较晚窗口先到、较早窗口后到）批次先进入重排缓冲，待连续前缀齐备后严格按
     * 窗口起点升序应用。返回值包含<b>本次调用实际新应用</b>的窗口产生的翻转事件
     * （被缓冲的窗口要等到它真正被应用的那次调用才体现在返回值里）。</p>
     */
    public List<GroupAlertEvent> evaluateWindow(long windowStart,
                                                List<WindowAggregation> sealedSeries) {
        List<WindowAggregation> snapshot =
                sealedSeries == null ? List.of() : List.copyOf(sealedSeries);
        List<GroupAlertEvent> produced = new ArrayList<>();

        lock.lock();
        try {
            // 幂等：窗口起点不大于已应用高水位的投递一律忽略。升序无空洞投递下这些只可能
            // 是重复回调；即便出现“比高水位更早的迟到投递”，也绝不能在状态已推进之后
            // 回头重放（那会造成重复翻转），直接忽略是唯一与顺序无关的安全选择。
            if (appliedHighWatermark != null && windowStart <= appliedHighWatermark) {
                return List.of();
            }
            // 尚未锚定：存储层总是从最早窗口开始升序投递，因此第一个到达的就是序列起点。
            // 直接使用判定器且要先投递较晚窗口的场景应先显式调用 anchor。
            if (appliedHighWatermark == null) {
                appliedHighWatermark = windowStart - windowSizeMs;
            }
            pendingWindows.put(windowStart, snapshot);
            flushContiguous(produced);
        } finally {
            lock.unlock();
        }
        return produced;
    }

    /**
     * 应用从“高水位下一窗口”开始的连续前缀；中间有空洞则停下等待更早窗口。空洞长期
     * 不补（达到重排缓冲上界）时放行缓冲中最小窗口以保持有界——存储层投递无空洞，
     * 正常路径不会触发强制放行。调用方必须持有 {@link #lock}。
     */
    private void flushContiguous(List<GroupAlertEvent> produced) {
        while (!pendingWindows.isEmpty()) {
            long next = appliedHighWatermark + windowSizeMs;
            List<WindowAggregation> batch = pendingWindows.remove(next);
            if (batch == null) {
                if (pendingWindows.size() >= maxPendingWindows) {
                    long forced = pendingWindows.firstKey();
                    batch = pendingWindows.remove(forced);
                    forcedFlushedWindows++;
                    applyWindow(forced, batch, produced);
                    continue;
                }
                break;
            }
            applyWindow(next, batch, produced);
        }
    }

    /**
     * 按窗口起点升序应用单个窗口，再对该窗口出现样本的各分组做滞回判定。整个过程在
     * {@link #lock} 内完成。
     *
     * <p><b>空窗口（缺样本补位）只推进窗口序列，不做任何状态动作</b>：不过期、不推进、
     * 不产生事件。保留期回收只在“有样本窗口”随该窗口判定一起做，与历史语义逐拍一致——
     * 分组连续缺席 {@code retention+1} 个<b>有样本</b>窗口后才淘汰，缺窗口本身不会提前
     * 淘汰 FIRING 状态。</p>
     */
    private void applyWindow(long windowStart, List<WindowAggregation> sealedSeries,
                             List<GroupAlertEvent> produced) {
        appliedHighWatermark = windowStart;
        if (sealedSeries.isEmpty()) {
            return;
        }
        long now = clock.millis();

        for (GroupAlertRule rule : rules) {
            // 物理回收已过保留期的状态，保证再次出现时按新组从 RESOLVED 开始（同时为状态
            // 条目数提供上界）。回收只随有样本窗口发生，缺窗口不改变任何分组的保留判定。
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
                // 本窗口该指标无样本：任何分组都不推进。
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

    /** 已按序应用到的最大窗口起点（诊断/测试用）；未锚定时为 {@link Long#MIN_VALUE}。 */
    public long appliedWatermark() {
        lock.lock();
        try {
            return appliedHighWatermark == null ? Long.MIN_VALUE : appliedHighWatermark;
        } finally {
            lock.unlock();
        }
    }

    /** 因投递长期存在空洞、超过重排缓冲上界而被迫放行的窗口数（正常路径恒为 0）。 */
    public long forcedFlushCount() {
        lock.lock();
        try {
            return forcedFlushedWindows;
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
