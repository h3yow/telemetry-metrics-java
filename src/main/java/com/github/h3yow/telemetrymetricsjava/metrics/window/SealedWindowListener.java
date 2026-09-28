package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.List;

/**
 * 窗口封定回调：存储层在窗口越过滚动边界、且迟到宽限也已结束时调用。
 *
 * <p>语义约定：</p>
 * <ul>
 *   <li>{@link #onWindowSealed} 每个（序列, 窗口）只回调一次；</li>
 *   <li>{@link #onWindowBatchSealed} 每个窗口只回调一次，入参是该窗口封定的
 *       <b>全部序列</b>快照（按序列键稳定排序），供分组告警做跨序列聚合。</li>
 * </ul>
 *
 * <p>单序列回调在存储锁内触发；批量回调在锁释放后触发，因此实现可以做聚合计算，
 * 但不得重入存储层。每个窗口只批量回调一次，窗口滚动/并发 tick 不会重复评估。</p>
 */
@FunctionalInterface
public interface SealedWindowListener {

    void onWindowSealed(WindowAggregation aggregation);

    /**
     * 某个窗口的全部序列快照已封定。默认空实现，分组告警侧覆写本方法。
     *
     * @param windowStart 窗口起点
     * @param aggregations 该窗口封定的全部序列聚合（非空、按序列键排序）
     */
    default void onWindowBatchSealed(long windowStart, List<WindowAggregation> aggregations) {
    }
}
