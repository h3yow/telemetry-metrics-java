package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvaluator;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertStatusView;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.FIRING;
import static com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.RESOLVED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分组判定器在<b>窗口投递乱序、重复、缺窗口</b>下的语义测试。
 *
 * <p>存储层在并发下保证升序、恰好一次投递；本测试把 GroupAlertEvaluator 当作独立组件，
 * 直接以打乱的窗口顺序、重复投递喂入，验证其结果与“严格按序喂入”完全一致：</p>
 * <ul>
 *   <li>较晚窗口先处理、较早窗口后到时，较早窗口不会被整段跳过，越线必触发；</li>
 *   <li>同一（分组 × 窗口）恰好判定一次，重复投递幂等忽略；</li>
 *   <li>缺样本窗口（空批次）不当 0 值推进、不过期；</li>
 *   <li>消失再出现保留期内沿用状态、超期按新组；恢复后重新越线重新触发；</li>
 *   <li>任意处理次序下最终状态与事件序列一致、可复现。</li>
 * </ul>
 */
class GroupAlertOutOfOrderEvaluationTest {

    private static final Logger log = LoggerFactory.getLogger(GroupAlertOutOfOrderEvaluationTest.class);

    private static final long T0 = 10_000;
    private static final long SIZE = 1_000;

    private static GroupAlertEvaluator newEvaluator(Clock clock, int retentionWindows) {
        return new GroupAlertEvaluator(List.of(
                new GroupAlertRule("grp", "cpu", List.of("host"),
                        AlertRule.Aggregator.MAX, 80.0, 20.0)),
                clock, 100, 100, retentionWindows, SIZE);
    }

    /** 构造窗口 w、host=a、单样本 value 的聚合快照。 */
    private static WindowAggregation agg(int w, double value) {
        return new WindowAggregation("cpu", Map.of("host", "a"),
                T0 + w * SIZE, SIZE, 1, value, value, value, value, T0 + w * SIZE + 100);
    }

    /** 每个窗口的样本水位：w0 正常 / w1 越线 / w2 越线 / w3 恢复 / w4 越线。 */
    private static double valueOf(int w) {
        return switch (w) {
            case 0 -> 70.0;
            case 1 -> 90.0;
            case 2 -> 85.0;
            case 3 -> 10.0;
            case 4 -> 95.0;
            default -> 50.0;
        };
    }

    /** 按严格升序投递 w0..w4（每个都有样本）的期望事件签名：w1 FIRE, w3 RESOLVE, w4 FIRE。 */
    private static List<String> expectedSignatures() {
        return List.of("FIRING@" + (T0 + SIZE), "RESOLVED@" + (T0 + 3 * SIZE),
                "FIRING@" + (T0 + 4 * SIZE));
    }

    private static List<String> signatures(List<GroupAlertEvent> events) {
        return events.stream().map(e -> e.state() + "@" + e.windowStart()).toList();
    }

    private static void assertFinalState(GroupAlertEvaluator ev, com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState state) {
        GroupAlertStatusView v = ev.currentStatuses().stream()
                .filter(s -> s.groupKey().equals("host=a")).findFirst().orElseThrow();
        assertEquals(state, v.state());
    }

    @Test
    void shuffledWindowOrderYieldsSameEventsAndStateAsInOrder() {
        MutableClock clock = new MutableClock(T0);

        // 对照组：严格按序。
        GroupAlertEvaluator ordered = newEvaluator(clock, 3);
        List<GroupAlertEvent> orderedEvents = new ArrayList<>();
        for (int w = 0; w <= 4; w++) {
            orderedEvents.addAll(ordered.evaluateWindow(T0 + w * SIZE, List.of(agg(w, valueOf(w)))));
        }

        // 乱序组：先纯声明序列最早窗口为 w0，随后故意把较晚窗口排在前面（4,2,1,3），
        // 最早窗口 w0 的数据最后补，补齐后 w0..w4 依次应用，事件与按序组完全一致。
        GroupAlertEvaluator shuffled = newEvaluator(clock, 3);
        List<GroupAlertEvent> shuffledEvents = new ArrayList<>();
        shuffled.anchor(T0);
        for (int w : new int[]{4, 2, 1, 3}) {
            shuffledEvents.addAll(shuffled.evaluateWindow(T0 + w * SIZE, List.of(agg(w, valueOf(w)))));
        }
        // 最后补 w0 的数据 -> 连续前缀齐备，缓冲的 w1..w4 依次升序应用。
        shuffledEvents.addAll(shuffled.evaluateWindow(T0, List.of(agg(0, valueOf(0)))));

        log.info("[乱序判定] 按序事件={}", signatures(orderedEvents));
        log.info("[乱序判定] 乱序事件={}", signatures(shuffledEvents));

        assertEquals(expectedSignatures(), signatures(orderedEvents));
        // 关键断言：乱序喂入得到完全相同的事件序列（状态/窗口/翻转）。
        assertEquals(signatures(orderedEvents), signatures(shuffledEvents),
                "窗口处理次序不同，事件结果必须一致");
        assertFinalState(ordered, FIRING);
        assertFinalState(shuffled, FIRING);
        assertEquals(0L, shuffled.forcedFlushCount(), "空洞补全后不应触发强制放行");
    }

    @Test
    void earlierWindowArrivingLateIsEvaluatedNotSkippedAndFires() {
        MutableClock clock = new MutableClock(T0);
        GroupAlertEvaluator ev = newEvaluator(clock, 3);

        // 声明最早窗口 w0（空）：之后先到的较晚窗口必须缓冲等待 w0 的数据。
        ev.anchor(T0);
        // w2（越线）先到 -> 因 w0/w1 未到，先缓冲，不产生事件。
        assertTrue(ev.evaluateWindow(T0 + 2 * SIZE, List.of(agg(2, valueOf(2)))).isEmpty());
        // w1（越线）后到 -> 仍缺 w0，继续缓冲。
        assertTrue(ev.evaluateWindow(T0 + 1 * SIZE, List.of(agg(1, valueOf(1)))).isEmpty());
        // w0 到达 -> 连续前缀齐备：w0(正常)->w1(越线 FIRE)->w2(仍 FIRING 无事件)，
        // 因此 w0 这次调用会返回被缓冲的 w1 触发事件。
        List<GroupAlertEvent> flushed =
                ev.evaluateWindow(T0, List.of(agg(0, valueOf(0))));
        assertEquals(1, flushed.size(), "较早窗口 w1 越线不得被整段跳过");
        assertEquals(FIRING, flushed.get(0).state());
        assertEquals(T0 + SIZE, flushed.get(0).windowStart());
        assertFinalState(ev, FIRING);
    }

    @Test
    void duplicateWindowDeliveryIsIdempotentExactlyOnce() {
        MutableClock clock = new MutableClock(T0);
        GroupAlertEvaluator ev = newEvaluator(clock, 3);
        ev.anchor(T0); // 声明最早窗口 w0：w1 先到时必须缓冲等待 w0
        List<GroupAlertEvent> first = ev.evaluateWindow(T0 + SIZE, List.of(agg(1, 90.0)));
        // 前驱 w0 缺失，w1 被缓冲，故首调用返回空；补 w0（正常）后 w1 触发。
        assertTrue(first.isEmpty());
        List<GroupAlertEvent> afterW0 = ev.evaluateWindow(T0, List.of(agg(0, 70.0)));
        assertEquals(1, afterW0.size());
        // 事件同时进入持久事件队列，取走清空。
        assertEquals(1, ev.drainEvents().size());

        // 重复投递已应用窗口（模拟并发/重复 tick）：必须幂等忽略，零事件、零副作用。
        for (int w : new int[]{0, 1}) {
            List<GroupAlertEvent> dup = ev.evaluateWindow(T0 + w * SIZE, List.of(agg(w, 99.0)));
            assertTrue(dup.isEmpty(), "重复窗口 w" + w + " 不得二次判定");
        }
        assertTrue(ev.drainEvents().isEmpty());
        assertFinalState(ev, FIRING);
        assertEquals(T0 + SIZE, ev.appliedWatermark());
    }

    @Test
    void emptyWindowsDoNotAdvanceOrExpireAndReappearKeepsStateEvenWhenShuffled() {
        MutableClock clock = new MutableClock(T0);
        GroupAlertEvaluator ev = newEvaluator(clock, 3);

        // 顺序：w0 越线 FIRE；w1、w2 空（缺样本）；w3 回来仍越线（沿用 FIRING，无事件）。
        // 故意乱序投递：先锚定最早窗口 w0（空），随后 w3(数据) 先到，w2(空)、w0(数据)、w1(空)。
        List<GroupAlertEvent> produced = new ArrayList<>();
        ev.anchor(T0);
        produced.addAll(ev.evaluateWindow(T0 + 3 * SIZE, List.of(agg(3, 85.0)))); // 缓冲
        produced.addAll(ev.evaluateWindow(T0 + 2 * SIZE, List.of()));             // 空，缓冲
        produced.addAll(ev.evaluateWindow(T0, List.of(agg(0, 90.0))));            // w0 应用，后续仍缺 w1
        produced.addAll(ev.evaluateWindow(T0 + SIZE, List.of()));                 // 空，补全 w1->w3

        // w0 FIRING；w1/w2 空不恢复不过期；w3 沿用 FIRING 不重复触发。
        assertEquals(1, produced.size());
        assertEquals(FIRING, produced.get(0).state());
        assertEquals(T0, produced.get(0).windowStart());
        assertFinalState(ev, FIRING);

        // w4 真正低于恢复阈值 -> 才产生 RESOLVED（证明空窗口没把它提前恢复）。
        List<GroupAlertEvent> recover = ev.evaluateWindow(T0 + 4 * SIZE, List.of(agg(4, 10.0)));
        assertEquals(1, recover.size());
        assertEquals(RESOLVED, recover.get(0).state());
    }

    @Test
    void goneBeyondRetentionReturnsAsNewGroupUnderOutOfOrderGaps() {
        MutableClock clock = new MutableClock(T0);
        GroupAlertEvaluator ev = newEvaluator(clock, 0); // 缺席 1 个有样本窗口即淘汰

        // w0 越线 FIRE。
        assertEquals(1, ev.evaluateWindow(T0, List.of(agg(0, 90.0))).size());
        // w1 空（缺样本，不过期）；w2 有数据：retention=0，w0 的状态按 w2 判定时已过期淘汰，
        // w2 值在死区 -> 按新分组从 RESOLVED 起，不触发。
        ev.evaluateWindow(T0 + SIZE, List.of());
        assertTrue(ev.evaluateWindow(T0 + 2 * SIZE, List.of(agg(2, 60.0))).isEmpty(),
                "超保留期回来按新组，死区值不触发");
        assertTrue(ev.currentStatuses().stream()
                .anyMatch(s -> s.groupKey().equals("host=a") && s.state() == RESOLVED));
        // w3 再次越线 -> 新触发。
        List<GroupAlertEvent> again = ev.evaluateWindow(T0 + 3 * SIZE, List.of(agg(3, 90.0)));
        assertEquals(1, again.size());
        assertEquals(FIRING, again.get(0).state());
    }

    @Test
    void recoverThenRearmProducesFreshFireRegardlessOfOrder() {
        MutableClock clock = new MutableClock(T0);
        GroupAlertEvaluator ev = newEvaluator(clock, 3);

        // 乱序：先锚定最早窗口 w0（空），再给 w3(恢复)、w1(越线)、w4(越线)、w0(正常)、w2(越线)。
        List<GroupAlertEvent> produced = new ArrayList<>();
        ev.anchor(T0);
        produced.addAll(ev.evaluateWindow(T0 + 3 * SIZE, List.of(agg(3, 10.0))));
        produced.addAll(ev.evaluateWindow(T0 + SIZE, List.of(agg(1, 90.0))));
        produced.addAll(ev.evaluateWindow(T0 + 4 * SIZE, List.of(agg(4, 95.0))));
        produced.addAll(ev.evaluateWindow(T0, List.of(agg(0, 70.0))));
        produced.addAll(ev.evaluateWindow(T0 + 2 * SIZE, List.of(agg(2, 85.0))));

        assertEquals(expectedSignatures(), signatures(produced),
                "恢复后重新越线必须重新触发，且与顺序无关");
        assertFinalState(ev, FIRING);
    }
}
