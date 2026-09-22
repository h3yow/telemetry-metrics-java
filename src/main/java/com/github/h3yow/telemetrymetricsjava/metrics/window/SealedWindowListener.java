package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

/**
 * 窗口封定回调：存储层在窗口越过滚动边界、且迟到宽限也已结束时调用一次。
 *
 * <p>语义约定：每个（序列, 窗口）只回调一次，因此告警状态机不会因窗口滚动
 * 或并发 tick 而重复评估同一窗口。实现不得回调重入存储层（存储层在持锁状态下调用）。</p>
 */
@FunctionalInterface
public interface SealedWindowListener {
    void onWindowSealed(WindowAggregation aggregation);
}
