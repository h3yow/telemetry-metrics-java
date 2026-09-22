package com.github.h3yow.telemetrymetricsjava.metrics.alert;

import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
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

    private final Map<String, AlertRule> rulesById = new HashMap<>();
    private final List<AlertRule> rules = new ArrayList<>();
    private final Clock clock;
    private final int maxEvents;

    private final ReentrantLock lock = new ReentrantLock();
    /** key = ruleId + '#' + 序列渲染名。 */
    private final Map<String, MutableStatus> statuses = new HashMap<>();
    private final Deque<AlertEvent> events = new ArrayDeque<>();
    private long droppedEvents;

    public AlertEvaluator(List<AlertRule> rules) {
        this(rules, Clock.systemUTC(), DEFAULT_MAX_EVENTS);
    }

    public AlertEvaluator(List<AlertRule> rules, Clock clock, int maxEvents) {
        this.clock = clock;
        this.maxEvents = maxEvents;
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
