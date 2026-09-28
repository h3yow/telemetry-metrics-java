package com.github.h3yow.telemetrymetricsjava.metrics.window;

import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;

import java.util.List;

/**
 * 分组窗口封定回调：同一个（指标名, 窗口起点）下、参与分组的全部序列快照，
 * 在窗口封定时由存储层<b>恰好一次</b>整体交出。
 *
 * <p>语义约定：
 * <ul>
 *   <li>每个（metric, windowStart）只回调一次，且发生在该窗口越过
 *       “结束 + 迟到宽限”封定线之后——此时迟到宽限内的样本已到齐，
 *       不会先回调一部分、稍后再回调剩余序列；</li>
 *   <li>{@code aggregations} 按序列键稳定排序，内容确定可复现；</li>
 *   <li>实现不得重入存储层（回调在存储锁内执行）。</li>
 * </ul></p>
 */
@FunctionalInterface
public interface GroupedWindowSealedListener {
    void onGroupedWindowSealed(String metricName, long windowStart, long windowSizeMs,
                               List<WindowAggregation> aggregations);
}
