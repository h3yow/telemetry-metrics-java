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
 *       <b>全部序列</b>快照（按序列键稳定排序），供分组告警做跨序列聚合；
 *       没有任何样本的窗口以<b>空列表</b>补位回调一次。</li>
 * </ul>
 *
 * <p>单序列回调在存储锁内触发；批量回调在锁外、由一把独立投递锁<b>串行</b>触发，
 * 且严格按窗口起点<b>升序、无空洞（空窗口补位）、不重复</b>——即使多个线程并发
 * 封定、较晚窗口先完成存储临界区，回调顺序仍与窗口时间顺序一致。实现可以在批量
 * 回调中做聚合计算，但不得重入存储层。</p>
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
