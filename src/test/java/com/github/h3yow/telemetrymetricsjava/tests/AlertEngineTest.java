package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.alert.AlertEngine;
import com.github.h3yow.telemetrymetricsjava.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.alert.AlertState;
import com.github.h3yow.telemetrymetricsjava.alert.ComparisonOp;
import com.github.h3yow.telemetrymetricsjava.alert.MetricFunction;
import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.model.WindowStats;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 告警状态机：阈值附近抖动不重复告警、状态不变不触发、恢复后再次越线重新触发、
 * 重复/过期窗口判定不会翻转结论。
 */
class AlertEngineTest {

    private static final Logger log = LoggerFactory.getLogger(AlertEngineTest.class);
    private static final SeriesKey KEY = new SeriesKey("cpu", Map.of("host", "h1"));
    private static final long W = 1_000L;

    /** avg=value 的单样本窗口，方便构造判定输入 */
    private static WindowStats window(long start, double avg) {
        return new WindowStats(start, start + W, 1, avg, avg, avg, avg);
    }

    private static AlertRule avgGt10Rule() {
        return new AlertRule("r1", "cpu", Map.of(), MetricFunction.AVG, ComparisonOp.GT, 10.0);
    }

    @Test
    void firesOnceWhileBreachingThenRecoversThenRefires() {
        AlertEngine engine = new AlertEngine(null);
        engine.addRule(avgGt10Rule());

        double[] sequence = {11, 12, 11, 9, 8, 11}; // 越 越 越 恢复 恢复 再越
        List<AlertEvent> all = new java.util.ArrayList<>();
        for (int i = 0; i < sequence.length; i++) {
            long start = i * W;
            var events = engine.evaluateWindow(KEY, window(start, sequence[i]));
            log.info("windowStart={} input avg={} -> events={} basis={}", start, sequence[i],
                    events.stream().map(e -> e.kind().name()).toList(),
                    engine.currentStatuses().stream().findFirst().map(s -> s.thresholdMessage()).orElse("-"));
            all.addAll(events);
        }

        assertEquals(3, all.size());
        assertEquals(AlertEvent.Kind.FIRING, all.get(0).kind());
        assertEquals(AlertEvent.Kind.RECOVERED, all.get(1).kind());
        assertEquals(AlertEvent.Kind.FIRING, all.get(2).kind(), "re-breach after recovery must re-fire");

        var status = engine.currentStatuses().get(0);
        assertEquals(AlertState.FIRING, status.state());
        log.info("final state FIRING with basis: {}", status.thresholdMessage());
    }

    @Test
    void thresholdJitterDoesNotRepeatWhileStateUnchanged() {
        AlertEngine engine = new AlertEngine(null);
        engine.addRule(avgGt10Rule());

        // 连续越线：只能产生 1 次 FIRING
        var e1 = engine.evaluateWindow(KEY, window(0, 10.5));
        var e2 = engine.evaluateWindow(KEY, window(W, 10.1));
        var e3 = engine.evaluateWindow(KEY, window(2 * W, 100));
        log.info("sustained breach events: {}, {}, {}", e1.size(), e2.size(), e3.size());
        assertEquals(1, e1.size() + e2.size() + e3.size());

        // 恢复一次，随后正常窗口不再产生事件，状态条目回归 OK 被清理
        var r1 = engine.evaluateWindow(KEY, window(3 * W, 9));
        var r2 = engine.evaluateWindow(KEY, window(4 * W, 8));
        assertEquals(1, r1.size());
        assertTrue(r2.isEmpty());
        assertEquals(0, engine.trackedEntries(), "OK state entries are pruned");
    }

    @Test
    void replayingSameOrOlderWindowCannotFlipConclusion() {
        AlertEngine engine = new AlertEngine(null);
        engine.addRule(avgGt10Rule());

        engine.evaluateWindow(KEY, window(W, 11));      // FIRING
        var replayNewerBreach = engine.evaluateWindow(KEY, window(W, 11)); // 同窗口重复
        var stale = engine.evaluateWindow(KEY, window(0, 5));             // 更旧窗口
        log.info("replay events={}, stale events={}, statuses={}",
                replayNewerBreach.size(), stale.size(), engine.currentStatuses().size());
        assertTrue(replayNewerBreach.isEmpty());
        assertTrue(stale.isEmpty());
        assertEquals(1, engine.trackedEntries());
        assertEquals(AlertState.FIRING, engine.currentStatuses().get(0).state());
    }

    @Test
    void rulesMatchByTagsAndRemovalClearsState() {
        AlertEngine engine = new AlertEngine(null);
        var h1Rule = new AlertRule("r-h1", "cpu", Map.of("host", "h1"), MetricFunction.AVG, ComparisonOp.GT, 10.0);
        engine.addRule(h1Rule);
        SeriesKey h2 = new SeriesKey("cpu", Map.of("host", "h2"));

        assertTrue(engine.evaluateWindow(h2, window(0, 99)).isEmpty(), "non-matching tags ignored");
        assertEquals(1, engine.evaluateWindow(KEY, window(0, 11)).size());

        assertTrue(engine.removeRule("r-h1"));
        assertEquals(0, engine.trackedEntries(), "removing a rule clears its state");
    }
}
