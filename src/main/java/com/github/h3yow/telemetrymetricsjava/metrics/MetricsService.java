package com.github.h3yow.telemetrymetricsjava.metrics;

import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertEvaluator;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertStatusView;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvaluator;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.GroupAlertStatusView;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupWindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.RejectReason;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import com.github.h3yow.telemetrymetricsjava.metrics.window.MetricWindowStore;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 指标门面：组装内存窗口存储与告警判定器，对外提供样本上报、窗口聚合查询、
 * 规则注册、告警状态/事件查询。
 *
 * <p>本类只做编排与参数校验，所有一致性保证（并发计数、窗口滚动、乱序归窗、
 * 告警状态稳定）均由 {@link MetricWindowStore} 与 {@link AlertEvaluator} 提供。</p>
 */
public class MetricsService {

    private final MetricsProperties properties;
    private final Clock clock;
    private final MetricWindowStore store;
    private final AlertEvaluator alertEvaluator;
    private final GroupAlertEvaluator groupAlertEvaluator;

    public MetricsService(MetricsProperties properties) {
        this(properties, Clock.systemUTC());
    }

    public MetricsService(MetricsProperties properties, Clock clock) {
        this.properties = properties;
        this.clock = clock;
        this.store = new MetricWindowStore(
                properties.getWindowSizeMs(),
                properties.getRetainedWindows(),
                properties.getMaxSamplesPerSeriesWindow(),
                properties.getMaxSeries(),
                properties.getMaxLabelValuesPerKey(),
                properties.getMaxFutureSkewMs(),
                properties.getLateArrivalGraceMs(),
                clock);
        this.alertEvaluator = new AlertEvaluator(List.of(), clock, 10_000);
        this.groupAlertEvaluator = new GroupAlertEvaluator(List.of(), clock,
                properties.getMaxGroupCombinations(), properties.getMaxSeriesPerGroup(),
                properties.getGroupAlertRetentionWindows(),
                properties.getWindowSizeMs(), 10_000);
        // 每个（序列，窗口）封定时恰好回调一次 -> 单序列规则每窗口只判定一次；
        // 每个窗口的全量序列快照只批量回调一次 -> 分组规则按组每窗口只判定一次。
        this.store.setSealedWindowListener(new com.github.h3yow.telemetrymetricsjava.metrics
                .window.SealedWindowListener() {
            @Override
            public void onWindowSealed(WindowAggregation agg) {
                alertEvaluator.evaluate(agg, agg.windowStart());
            }

            @Override
            public void onWindowBatchSealed(long windowStart, List<WindowAggregation> aggregations) {
                // 存储层保证周期严格按窗口升序投递；水位在同周期末尾统一通知，
                // 这里先按窗口自身水位暂存/应用，乱序防御在判定器内完成。
                groupAlertEvaluator.evaluateWindow(windowStart, aggregations, windowStart);
            }

            @Override
            public void onSealHorizonAdvanced(long sealHorizonWindowStart) {
                groupAlertEvaluator.advanceSealWatermark(sealHorizonWindowStart);
            }
        });
    }

    /** 上报单条样本，返回带明确拒绝原因的结果。 */
    public IngestResult ingest(MetricSample sample) {
        return store.ingest(sample);
    }

    /**
     * 批量上报：逐条独立判定，单条被拒不影响其它样本；结果顺序与输入顺序一致，
     * 便于调用方按位置定位每条样本的归属窗口或拒绝原因。
     */
    public List<IngestResult> ingestBatch(List<MetricSample> samples) {
        if (samples == null) {
            throw new IllegalArgumentException("samples must not be null");
        }
        List<IngestResult> out = new ArrayList<>(samples.size());
        for (MetricSample sample : samples) {
            out.add(store.ingest(sample));
        }
        return out;
    }

    /**
     * 查询某序列在时间范围覆盖到的窗口聚合（端点按窗口起点，闭区间）。
     * 时间戳会先对齐到窗口边界。
     */
    public List<WindowAggregation> query(String name, Map<String, String> labels,
                                         long fromEpochMs, long toEpochMs) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("metric name must be present");
        }
        if (fromEpochMs > toEpochMs) {
            throw new IllegalArgumentException("fromEpochMs must be <= toEpochMs");
        }
        long fromWindow = Math.floorDiv(fromEpochMs, properties.getWindowSizeMs())
                * properties.getWindowSizeMs();
        long toWindow = Math.floorDiv(toEpochMs, properties.getWindowSizeMs())
                * properties.getWindowSizeMs();
        return store.query(name, labels, fromWindow, toWindow);
    }

    /** 注册告警规则；重复 ruleId 将抛出 IllegalArgumentException。 */
    public AlertRule registerRule(AlertRule rule) {
        alertEvaluator.addRule(rule);
        return rule;
    }

    /**
     * 按部分标签分组查询：同一窗口内 groupBy 键取值相同的多条序列合并为一组，
     * 返回每个有样本的（窗口 × 分组）的跨序列聚合。未参与分组的标签不会拆分组；
     * groupBy 为空表示整指标一个全局组。结果按窗口起点、分组标签稳定排序，可复现。
     * 触及任一分组容量上界时抛出带可区分原因的 GroupLimitException（不静默截断/合并）。
     */
    public List<GroupWindowAggregation> queryGrouped(String name, List<String> groupBy,
                                                     long fromEpochMs, long toEpochMs) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("metric name must be present");
        }
        if (groupBy == null) {
            throw new IllegalArgumentException("groupBy must not be null");
        }
        if (fromEpochMs > toEpochMs) {
            throw new IllegalArgumentException("fromEpochMs must be <= toEpochMs");
        }
        long size = properties.getWindowSizeMs();
        long fromWindow = Math.floorDiv(fromEpochMs, size) * size;
        long toWindow = Math.floorDiv(toEpochMs, size) * size;
        return store.queryGrouped(name, List.copyOf(new java.util.TreeSet<>(groupBy)),
                fromWindow, toWindow,
                properties.getMaxGroupCombinations(), properties.getMaxSeriesPerGroup(),
                properties.getMaxGroupedQueryRows(), properties.getMaxQueryWindows());
    }

    /** 注册分组告警规则；重复 ruleId 将抛出 IllegalArgumentException。 */
    public GroupAlertRule registerGroupRule(GroupAlertRule rule) {
        groupAlertEvaluator.addRule(rule);
        return rule;
    }

    /** 当前全部规则 × 分组的滞回状态快照（稳定排序）。 */
    public List<GroupAlertStatusView> groupAlertStatuses() {
        return groupAlertEvaluator.currentStatuses();
    }

    /** 取走并清空累积的分组告警翻转事件。 */
    public List<GroupAlertEvent> drainGroupAlertEvents() {
        return groupAlertEvaluator.drainEvents();
    }

    /** 分组规则因容量越界被跳过的规则×窗口评估次数（诊断用）。 */
    public long groupAlertRejectedCount() {
        return groupAlertEvaluator.rejectedEvaluationCount();
    }

    /** 最近的分组容量越界明细（ruleId#windowStart -> 原因/上限/实际值）。 */
    public Map<String, String> groupAlertLastRejections() {
        return groupAlertEvaluator.lastRejections();
    }

    public List<AlertStatusView> alertStatuses() {
        return alertEvaluator.currentStatuses();
    }

    /** 取走并清空累积的状态翻转事件。 */
    public List<AlertEvent> drainAlertEvents() {
        return alertEvaluator.drainEvents();
    }

    /**
     * 显式推进窗口维护（封定/淘汰）。真实运行时由定时任务周期调用；
     * 测试中用固定/可调时钟后调用本方法以确定性地触发窗口滚动与告警判定。
     */
    public void maintain() {
        store.maintain();
    }

    /** 便捷构造样本。 */
    public static MetricSample sample(String name, double value, long timestamp,
                                      Map<String, String> labels, String sampleId) {
        return new MetricSample(name, value, timestamp, labels, sampleId);
    }

    /** 便捷构造拒绝判定（测试/外部实现复用拒绝原因枚举）。 */
    public static IngestResult rejected(RejectReason reason, String detail) {
        return IngestResult.rejected(reason, detail);
    }

    Clock clock() {
        return clock;
    }

    MetricWindowStore store() {
        return store;
    }

    AlertEvaluator alertEvaluator() {
        return alertEvaluator;
    }
}
