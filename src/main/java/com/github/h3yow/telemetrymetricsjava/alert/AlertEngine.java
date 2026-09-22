package com.github.h3yow.telemetrymetricsjava.alert;

import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.model.WindowStats;
import com.github.h3yow.telemetrymetricsjava.window.WindowStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费已关闭窗口，按规则做带滞回（hysteresis）的状态机判定。
 *
 * <p>状态迁移（每个 (ruleId, series) 独立维护）：</p>
 * <pre>
 *   (无记录=OK) --越线--> FIRING(发事件)
 *   FIRING      --持续越线--> FIRING（不发事件）
 *   FIRING      --恢复--> RECOVERED(发事件)
 *   RECOVERED   --持续恢复--> 回到 OK（不发事件）
 *   RECOVERED   --再次越线--> FIRING(重新发事件)
 * </pre>
 * <p>因此阈值附近抖动只会在迁移点通知一次；恢复后再次越线必然重新通知。</p>
 *
 * <p>状态条数上界 = 规则数 × 序列数，而序列数受 WindowStore 的 maxSeries 约束，
 * 规则数受 {@link #MAX_RULES} 约束，故状态内存有界。</p>
 */
public class AlertEngine {

    /** 规则数量硬上界，防止状态空间无界增长 */
    public static final int MAX_RULES = 1_000;

    private final WindowStore windowStore;
    private final Map<String, AlertRule> rules = new ConcurrentHashMap<>();
    private final Map<StateKey, StateEntry> states = new ConcurrentHashMap<>();

    public AlertEngine(WindowStore windowStore) {
        this.windowStore = windowStore;
    }

    private record StateKey(String ruleId, SeriesKey series) {
    }

    private record StateEntry(AlertState state, long updatedWindowStartMs, double lastObservedValue,
                              String message) {
    }

    public void addRule(AlertRule rule) {
        if (rules.containsKey(rule.ruleId())) {
            throw new IllegalArgumentException("rule already exists: " + rule.ruleId());
        }
        if (rules.size() >= MAX_RULES) {
            throw new IllegalStateException("rule limit " + MAX_RULES + " reached");
        }
        rules.put(rule.ruleId(), rule);
    }

    public boolean removeRule(String ruleId) {
        boolean removed = rules.remove(ruleId) != null;
        if (removed) {
            states.keySet().removeIf(k -> k.ruleId().equals(ruleId));
        }
        return removed;
    }

    public List<AlertRule> rules() {
        return List.copyOf(rules.values());
    }

    public int trackedEntries() {
        return states.size();
    }

    private static String explain(AlertRule rule, double observed, boolean breached, long windowStart) {
        return "windowStart=" + windowStart + " " + rule.function() + "=" + round(observed)
                + " " + rule.op() + " threshold=" + round(rule.threshold())
                + " => " + (breached ? "BREACHED" : "WITHIN");
    }

    private static double round(double v) {
        return Math.round(v * 1_000_000.0d) / 1_000_000.0d;
    }

    /**
     * 用一个已关闭窗口评估所有匹配规则。窗口按水位顺序到达；窗口起点不新于已知状态的
     * 调用被视为过期/重复输入而跳过，保证乱序/重复输入不会翻转已得出的结论。
     */
    public List<AlertEvent> evaluateWindow(SeriesKey key, WindowStats stats) {
        List<AlertEvent> events = new ArrayList<>();
        for (AlertRule rule : rules.values()) {
            if (!rule.matches(key)) {
                continue;
            }
            double observed = rule.function().extract(stats);
            boolean breached = rule.breachedBy(observed);
            StateKey sk = new StateKey(rule.ruleId(), key);
            String message = explain(rule, observed, breached, stats.windowStartMs());

            states.compute(sk, (k, prev) -> {
                if (prev != null && stats.windowStartMs() <= prev.updatedWindowStartMs()) {
                    // 过期或重复窗口：维持原结论
                    return prev;
                }
                AlertState prevState = prev == null ? AlertState.OK : prev.state();
                return switch (prevState) {
                    case OK, RECOVERED -> {
                        if (breached) {
                            events.add(new AlertEvent(AlertEvent.Kind.FIRING, rule.ruleId(), key,
                                    stats.windowStartMs(), observed, message));
                            yield new StateEntry(AlertState.FIRING, stats.windowStartMs(), observed, message);
                        }
                        if (prevState == AlertState.RECOVERED) {
                            // 连续恢复：回到 OK，释放状态条目
                            yield null;
                        }
                        // OK 且未越线：保持无记录
                        yield prev;
                    }
                    case FIRING -> {
                        if (breached) {
                            // 持续越线：状态不变，不重复告警；仅刷新判定依据
                            yield new StateEntry(AlertState.FIRING, stats.windowStartMs(), observed, message);
                        }
                        events.add(new AlertEvent(AlertEvent.Kind.RECOVERED, rule.ruleId(), key,
                                stats.windowStartMs(), observed, message));
                        yield new StateEntry(AlertState.RECOVERED, stats.windowStartMs(), observed, message);
                    }
                };
            });
            // compute 返回 null 表示状态回归 OK，条目已从 map 移除，无需额外处理
        }
        return events;
    }

    public List<AlertStatusView> currentStatuses() {
        List<AlertStatusView> views = new ArrayList<>();
        for (Map.Entry<StateKey, StateEntry> e : states.entrySet()) {
            StateEntry v = e.getValue();
            views.add(new AlertStatusView(e.getKey().ruleId(), e.getKey().series(), v.state(),
                    v.updatedWindowStartMs(), v.lastObservedValue(), v.message()));
        }
        return views;
    }
}
