package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvaluator;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.FIRING;
import static com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertState.RESOLVED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 分组告警“窗口判定顺序被打乱”专项测试（直接驱动 {@link GroupAlertEvaluator}）：
 *
 * <ol>
 *   <li>窗口回调按任意打乱顺序到达、并诚实告知封定水位时，最终事件序列与分组状态
 *       必须与严格按窗口升序处理完全一致（不漏判、不重复判）；</li>
 *   <li>同窗口重复回调（并发重投）幂等；</li>
 *   <li>缺样本窗口（整窗无回调）不推进状态；</li>
 *   <li>恢复后再次越线仍然重新触发；</li>
 *   <li>多分组在乱序下互不干扰。</li>
 * </ol>
 */
class GroupAlertOutOfOrderTest {

    private static final Logger log = LoggerFactory.getLogger(GroupAlertOutOfOrderTest.class);

    private static final long SIZE = 1_000L;
    private static final long T0 = 100_000L;

    /** 构造某窗口某 host 单序列、单样本的聚合快照（AVG/MAX/MIN/LAST 都等于该值）。 */
    private static WindowAggregation agg(int w, String host, double value) {
        return new WindowAggregation("cpu", Map.of("host", host), T0 + w * SIZE, SIZE,
                1L, value, value, value, value, T0 + w * SIZE + 100L);
    }

    private static List<WindowAggregation> batch(int w, String... hostValuePairs) {
        List<WindowAggregation> out = new ArrayList<>();
        for (int i = 0; i < hostValuePairs.length; i += 2) {
            out.add(agg(w, hostValuePairs[i], Double.parseDouble(hostValuePairs[i + 1])));
        }
        return out;
    }

    private static GroupAlertEvaluator newEvaluator(int retentionWindows) {
        GroupAlertRule rule = new GroupAlertRule("r", "cpu", List.of("host"),
                AlertRule.Aggregator.MAX, 80.0, 50.0);
        // 时钟固定在最后一个数据窗口结束后（生产中判定紧跟实时窗口，视图按时钟过滤等价）
        return new GroupAlertEvaluator(List.of(rule), new MutableClock(T0 + 5 * SIZE),
                100, 100, retentionWindows, SIZE);
    }

    /** 事件的可比较签名：窗口#分组#翻转后状态。 */
    private static List<String> signatures(List<GroupAlertEvent> events) {
        return events.stream()
                .map(e -> (e.windowStart() - T0) / SIZE + "#" + e.groupKey() + "#" + e.state())
                .sorted()
                .toList();
    }

    /**
     * 场景数据：w0..w4 中 host=a 的取值依次为 90(触发)、95(死区)、90(死区)、
     * 40(恢复)、90(重新触发)；host=b 全程 10(从不触发)。
     */
    private static List<List<WindowAggregation>> scenario() {
        return List.of(
                batch(0, "a", "90", "b", "10"),
                batch(1, "a", "95", "b", "10"),
                batch(2, "a", "90", "b", "10"),
                batch(3, "a", "40", "b", "10"),
                batch(4, "a", "90", "b", "10"));
    }

    @Test
    void shuffledWindowDeliveryYieldsExactlySameResultAsInOrder() {
        // 基准：严格按窗口升序处理
        GroupAlertEvaluator ordered = newEvaluator(3);
        List<List<WindowAggregation>> data = scenario();
        List<GroupAlertEvent> orderedEvents = new ArrayList<>();
        for (int w = 0; w < data.size(); w++) {
            // 顺序投递：水位就是当前窗口本身
            orderedEvents.addAll(ordered.evaluateWindow(T0 + w * SIZE, data.get(w), T0 + w * SIZE));
        }
        List<String> expected = signatures(orderedEvents);
        log.info("[乱序] 顺序基准事件签名: {}", expected);
        assertEquals(List.of("0#host=a#FIRING", "3#host=a#RESOLVED", "4#host=a#FIRING"),
                expected);

        // 乱序：窗口 2、0、4、1、3；每次水位为“已封定周期的上界”。
        // 语义为：调用 evaluateWindow(w, horizon) 时，所有 <= horizon 的有样本窗口
        // 最终都会送达（允许在途乱序），< horizon 的缺失窗口即为缺样本窗口。
        // 乱序：窗口 2、0、4、1、3；水位（第二列）诚实表示“≤ 该水位的有样本窗口
        // 都已送达，更老的未送达窗口只可能是缺样本窗口”。更老窗口仍在途时水位不得推进，
        // 因此先到的 w2/w4 必须缓冲，状态机不会越过在途的更老窗口提前判定。
        int[][] delivery = {
                {2, -1},  // w2 先到，但 w0/w1 在途：水位停在“尚无穷”，w2 缓冲
                {0, 0},   // w0 到达：应用 w0（触发）
                {4, 0},   // w4 先到（w1/w3 在途）：继续缓冲
                {1, 2},   // w1 到达，水位推进到 w2：补刷 w1、w2（死区，无事件）
                {3, 4}    // w3 到达，水位推进到 w4：补刷 w3（恢复）、w4（重新触发）
        };
        GroupAlertEvaluator shuffled = newEvaluator(3);
        List<GroupAlertEvent> shuffledEvents = new ArrayList<>();
        for (int[] d : delivery) {
            List<GroupAlertEvent> out = shuffled.evaluateWindow(
                    T0 + d[0] * SIZE, data.get(d[0]), T0 + d[1] * SIZE);
            log.info("[乱序] 投递 w{} 水位 w{} -> 本次产出 {}", d[0], d[1], signatures(out));
            shuffledEvents.addAll(out);
        }

        // 事件按状态实际应用（事件时间）顺序产出：与顺序基准逐条一致
        List<String> actual = signatures(shuffledEvents);
        log.info("[乱序] 乱序投递最终事件签名: {}", actual);
        assertEquals(expected, actual, "乱序处理的事件必须与顺序处理完全一致");
        // 状态：a 在 w4 重新触发后为 FIRING（host=b 从未翻转，不会产生状态条目）
        com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertStatusView sa =
                shuffled.currentStatuses().stream()
                        .filter(v -> v.groupKey().equals("host=a")).findFirst().orElseThrow();
        assertEquals(FIRING, sa.state());
        assertEquals(T0 + 4 * SIZE, sa.lastEvaluatedWindowStart());
    }

    @Test
    void duplicateAndLateRedeliveryOfSameWindowIsIdempotent() {
        GroupAlertEvaluator ev = newEvaluator(3);
        List<List<WindowAggregation>> data = scenario();

        // 顺序投递 w0、w1，w0 在应用后又被并发/重试重复投递，必须被幂等忽略。
        List<GroupAlertEvent> out = new ArrayList<>();
        out.addAll(ev.evaluateWindow(T0, data.get(0), T0));
        out.addAll(ev.evaluateWindow(T0 + SIZE, data.get(1), T0 + SIZE));
        out.addAll(ev.evaluateWindow(T0, data.get(0), T0)); // 已应用窗口的迟到重投
        assertEquals(List.of("0#host=a#FIRING"), signatures(out));

        // 乱序重复：w3 先到并缓冲，w3 的重复回调先于 w1/w2 到达
        out.clear();
        out.addAll(ev.evaluateWindow(T0 + 3 * SIZE, data.get(3), T0 + SIZE));
        out.addAll(ev.evaluateWindow(T0 + 3 * SIZE, data.get(3), T0 + SIZE)); // 在途重复
        assertTrue(out.isEmpty(), "更老窗口未补齐前，缓冲窗口不得提前产出事件");
        out.addAll(ev.evaluateWindow(T0 + 2 * SIZE, data.get(2), T0 + 3 * SIZE));
        // 补刷 w2（无事件）、w3（恢复）；w3 只判定一次
        assertEquals(List.of("3#host=a#RESOLVED"), signatures(out));

        // w0 再投一次（远早于已应用水位）仍然无副作用
        assertTrue(ev.evaluateWindow(T0, data.get(0), T0).isEmpty());
        assertEquals(List.of("0#host=a#FIRING", "3#host=a#RESOLVED"),
                signatures(ev.drainEvents()));
    }

    @Test
    void missingWindowsAndDisappearReappearAndRearmAreStableUnderShuffle() {
        // retention=2：缺席严格超过 2 个窗口才回收。
        // w0 触发；w1 缺样本（不恢复、不回收）；w2 回来沿用 FIRING（不重复触发）；
        // w3、w4 连续缺席 -> 超保留期回收；w5 回来作为新组从 RESOLVED 起、死区值不触发；
        // w6 再次越线 -> 新触发。
        int[] values = {90, -1, 90, -1, -1, 60, 90}; // -1 = 该窗口该分组无样本

        // 顺序基准
        GroupAlertEvaluator ordered = newEvaluator(2);
        List<GroupAlertEvent> oe = new ArrayList<>();
        for (int w = 0; w < values.length; w++) {
            // 顺序路径：有样本窗口带数据 + 水位；缺样本窗口只推进水位
            if (values[w] >= 0) {
                oe.addAll(ordered.evaluateWindow(T0 + w * SIZE,
                        List.of(agg(w, "a", values[w])), T0 + w * SIZE));
            } else {
                oe.addAll(ordered.advanceSealWatermark(T0 + w * SIZE));
            }
        }
        List<String> expected = signatures(oe);
        log.info("[乱序-缺窗] 顺序基准事件: {}", expected);
        assertEquals(List.of("0#host=a#FIRING", "6#host=a#FIRING"), expected);

        // 乱序路径：有样本窗口 5、2、6、0 以打乱顺序先到，缺样本窗口只通过水位告知。
        GroupAlertEvaluator shuffled = newEvaluator(2);
        List<GroupAlertEvent> se = new ArrayList<>();
        se.addAll(shuffled.evaluateWindow(T0 + 5 * SIZE, List.of(agg(5, "a", 60)), T0 - SIZE));
        se.addAll(shuffled.evaluateWindow(T0 + 2 * SIZE, List.of(agg(2, "a", 90)), T0 - SIZE));
        se.addAll(shuffled.evaluateWindow(T0 + 6 * SIZE, List.of(agg(6, "a", 90)), T0 - SIZE));
        assertTrue(se.isEmpty(), "更老窗口未确认到齐前，先到的晚窗口必须全部缓冲、零事件");

        // w0 到达，水位推进到 w0：应用 w0（触发）
        se.addAll(shuffled.evaluateWindow(T0, List.of(agg(0, "a", 90)), T0));
        assertEquals(List.of("0#host=a#FIRING"), signatures(se));

        // 依次用“封定周期完成”通知补齐缺样本缺口，晚窗口随水位按序补刷
        List<GroupAlertEvent> rest = new ArrayList<>();
        rest.addAll(shuffled.advanceSealWatermark(T0 + 1 * SIZE)); // w1 缺样本
        rest.addAll(shuffled.advanceSealWatermark(T0 + 2 * SIZE)); // 应用 w2：沿用 FIRING
        rest.addAll(shuffled.advanceSealWatermark(T0 + 3 * SIZE)); // w3 缺样本
        rest.addAll(shuffled.advanceSealWatermark(T0 + 4 * SIZE)); // w4 缺样本：w2 状态超期回收
        rest.addAll(shuffled.advanceSealWatermark(T0 + 5 * SIZE)); // 应用 w5：新组 RESOLVED 死区
        rest.addAll(shuffled.advanceSealWatermark(T0 + 6 * SIZE)); // 应用 w6：重新触发
        log.info("[乱序-缺窗] 补刷各水位产出: {}", signatures(rest));
        assertEquals(List.of("6#host=a#FIRING"), signatures(rest),
                "w2 沿用 FIRING 不重复、w5 新组死区不触发、w6 重新触发");

        // 全量事件与顺序基准完全一致（集合与内容）
        List<String> all = new ArrayList<>();
        all.addAll(signatures(se));
        all.addAll(signatures(rest));
        assertEquals(expected, all.stream().sorted().toList());

        // 最终状态为 w6 触发后的 FIRING，最近判定窗口是 w6
        var status = shuffled.currentStatuses().stream()
                .filter(v -> v.groupKey().equals("host=a")).findFirst().orElseThrow();
        assertEquals(FIRING, status.state());
        assertEquals(T0 + 6 * SIZE, status.lastEvaluatedWindowStart());

        // 水位重复推进 / 已应用窗口重投均无副作用
        assertTrue(shuffled.advanceSealWatermark(T0 + 6 * SIZE).isEmpty());
        assertTrue(shuffled.evaluateWindow(T0 + 2 * SIZE, List.of(agg(2, "a", 90)),
                T0 + 6 * SIZE).isEmpty());
    }
}
