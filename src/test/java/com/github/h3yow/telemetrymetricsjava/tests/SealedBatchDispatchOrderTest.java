package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.window.MetricWindowStore;
import com.github.h3yow.telemetrymetricsjava.metrics.window.SealedWindowListener;
import com.github.h3yow.telemetrymetricsjava.tests.support.MutableClock;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 封定批次投递顺序测试（缺陷根因的确定性复现）：
 *
 * <p>旧实现中，每个维护线程把自己封定到的窗口批次放在<b>线程本地</b>容器里，在释放
 * 存储锁之后才回调分组判定器。两个线程并发时，封定较晚窗口（w1）的线程可能先执行
 * 锁外回调，封定较早窗口（w0）的线程后回调——判定器再用“窗口必须严格递增”的保护
 * 把晚到的 w0 整段跳过，造成“明明越线却一个事件都没有”。</p>
 *
 * <p>本测试用两个线程精确构造该竞态：线程 A 封定 w0 后在<b>投递阶段</b>被拦住，
 * 线程 B 此时封定并投递 w1；随后放行 A。修复后批量回调必须仍严格按窗口起点升序
 * （w0 先于 w1）、串行、且每窗口恰好一次（含缺样本窗口的空补位）。</p>
 */
class SealedBatchDispatchOrderTest {

    private static final Logger log = LoggerFactory.getLogger(SealedBatchDispatchOrderTest.class);

    private static MetricWindowStore newStore(Clock clock) {
        return new MetricWindowStore(1_000, 10, 100_000, 100, 100,
                60_000, 0, clock);
    }

    @Test
    void concurrentSealingDispatchesBatchesInAscendingWindowOrderExactlyOnce() throws Exception {
        long t0 = 100_000;
        MutableClock clock = new MutableClock(t0);
        MetricWindowStore store = newStore(clock);

        // 记录实际批量回调顺序（线程安全：旧实现允许回调并发，修复后回调串行）。
        List<Long> dispatchOrder = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Integer> concurrentCallbacks = java.util.Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> listenerFailure = new AtomicReference<>();
        // w0 回调尚未结束时，是否有其它窗口（w1）的回调已经开始（旧实现会，修复后不会）。
        java.util.concurrent.atomic.AtomicBoolean laterWindowDispatchedDuringEarlier =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicBoolean w0InProgress =
                new java.util.concurrent.atomic.AtomicBoolean(false);

        // A 封定 w0 后会卡在 w0 的批量回调上，等待放行；B 在此期间封定并投递 w1、w2。
        CountDownLatch reachedW0 = new CountDownLatch(1);
        CountDownLatch releaseW0 = new CountDownLatch(1);

        store.setSealedWindowListener(new SealedWindowListener() {
            // 无锁的并发探测器：回调正确串行时 active 只可能在 0/1 之间变化；
            // 若两个回调并发执行，active 会瞬时达到 2。
            private final java.util.concurrent.atomic.AtomicInteger active =
                    new java.util.concurrent.atomic.AtomicInteger();

            @Override
            public void onWindowSealed(WindowAggregation aggregation) {
                // 单序列回调，忽略（顺序由存储锁保证）。
            }

            @Override
            public void onWindowBatchSealed(long windowStart,
                                           List<WindowAggregation> aggregations) {
                int nowActive = active.incrementAndGet();
                try {
                    if (nowActive > 1) {
                        concurrentCallbacks.add(nowActive);
                    }
                    if (windowStart != t0 && w0InProgress.get()) {
                        laterWindowDispatchedDuringEarlier.set(true);
                    }
                    dispatchOrder.add(windowStart);
                    log.info("[投递顺序] 收到窗口 {} 批次大小={} 并发数={}",
                            windowStart, aggregations.size(), nowActive);
                    if (windowStart == t0) {
                        w0InProgress.set(true);
                        reachedW0.countDown();
                        // 带着回调进行中状态等待：若投递不是串行的，B 会在此期间插进来。
                        if (!releaseW0.await(5, TimeUnit.SECONDS)) {
                            listenerFailure.compareAndSet(null,
                                    new AssertionError("测试同步信号未被放行，疑似死锁"));
                        }
                        w0InProgress.set(false);
                    }
                } catch (Throwable t) {
                    listenerFailure.compareAndSet(null, t);
                } finally {
                    active.decrementAndGet();
                }
            }
        });

        // w0、w1 各放一条样本，让两个线程都封定到非空批次（直接复现“较晚窗口先投递”）。
        assertTrue(store.ingest(new MetricSample("cpu", 90.0, t0 + 100,
                Map.of("host", "a"), "w0")).accepted());
        assertTrue(store.ingest(new MetricSample("cpu", 80.0, t0 + 1_100,
                Map.of("host", "a"), "w1")).accepted());

        Thread a = new Thread(() -> {
            clock.setTime(t0 + 1_000); // sealHorizon = t0 -> 封定 w0
            store.maintain();
        }, "seal-w0");
        Thread b = new Thread(() -> {
            try {
                // 等 A 已经进入 w0 的批量回调（投递锁被 A 持有）。
                assertTrue(reachedW0.await(5, TimeUnit.SECONDS));
                // B 推进到 w1 封定点：sealHorizon=t0+1000，补位空窗口 w1 并入队；
                // B 的投递会阻塞在投递锁上，直到 A 放行后由 A 统一升序排空。
                clock.setTime(t0 + 2_000);
                store.maintain();
            } catch (Throwable t) {
                listenerFailure.compareAndSet(null, t);
            }
        }, "seal-w1w2");

        a.start();
        b.start();
        assertTrue(reachedW0.await(5, TimeUnit.SECONDS));
        // 给 B 充分时间完成它的存储临界区并入队 w1/w2（B 的投递会阻塞在投递锁上）。
        Thread.sleep(200);
        releaseW0.countDown();
        a.join(5_000);
        b.join(5_000);
        assertTrue(!a.isAlive() && !b.isAlive(), "维护线程必须正常结束");
        if (listenerFailure.get() != null) {
            throw new AssertionError("listener 内断言失败", listenerFailure.get());
        }

        log.info("[投递顺序] 最终回调窗口序列 = {}", dispatchOrder);
        // 关键断言：较早窗口（w0）的回调结束前，较晚窗口（w1）不得开始回调。
        assertTrue(!laterWindowDispatchedDuringEarlier.get(),
                "较晚窗口不得在较早窗口回调结束前投递（旧实现的漏判根因）");
        // 关键断言：即使 w1/w2 先被另一个线程封定完成，回调顺序仍必须严格升序。
        List<Long> sorted = new ArrayList<>(dispatchOrder);
        assertEquals(sorted, dispatchOrder, "批量回调必须严格按窗口起点升序");
        assertEquals(2, dispatchOrder.size(),
                "应恰好投递 w0(数据)、w1(数据)，共 2 个窗口，实际: " + dispatchOrder);
        assertEquals(t0, dispatchOrder.get(0));
        assertEquals(t0 + 1_000, dispatchOrder.get(1));
        // 串行性：回调进行中计数必须始终为 0 或 1（没有两个回调并发执行）。
        assertTrue(concurrentCallbacks.stream().allMatch(n -> n <= 1),
                "批量回调必须串行执行");
        // 先把时钟推进到更远处，把新封定的窗口一次性排空。
        clock.setTime(t0 + 4_000);
        store.maintain();
        int before = dispatchOrder.size();
        // 时钟不动，连续重复维护若干次，不得再投递任何已封定窗口（每窗口恰好一次）。
        for (int i = 0; i < 3; i++) {
            store.maintain();
        }
        assertEquals(before, dispatchOrder.size(), "重复 tick 不得重复投递已封定窗口");
    }
}
