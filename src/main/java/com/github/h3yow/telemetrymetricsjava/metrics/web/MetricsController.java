package com.github.h3yow.telemetrymetricsjava.metrics.web;

import com.github.h3yow.telemetrymetricsjava.metrics.MetricsService;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.metrics.alert.AlertStatusView;
import com.github.h3yow.telemetrymetricsjava.metrics.model.GroupQueryResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.metrics.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.metrics.model.WindowAggregation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 本地指标 HTTP 接口（可选使用；核心能力均可在单测中直接调用，无外部依赖）。
 */
@RestController
@RequestMapping("/metrics")
public class MetricsController {

    private final MetricsService metricsService;

    public MetricsController(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    /** 批量上报样本；每条样本独立给出接受/拒绝结论与原因。 */
    @PostMapping("/samples")
    public List<IngestResult> ingest(@RequestBody List<MetricSample> samples) {
        return metricsService.ingestBatch(samples);
    }

    /** 查询某序列在时间范围内各窗口的聚合。 */
    @PostMapping("/query")
    public List<WindowAggregation> query(@RequestBody QueryRequest request) {
        return metricsService.query(request.name(),
                request.labels() == null ? Map.of() : request.labels(),
                request.fromEpochMs(), request.toEpochMs());
    }

    /** 按部分标签分组查询跨序列聚合。超限时 HTTP 422 + 可区分状态，不静默截断。 */
    @PostMapping("/query-groups")
    public ResponseEntity<GroupQueryResult> queryGroups(@RequestBody GroupQueryRequest request) {
        GroupQueryResult result = metricsService.queryGroups(request.name(),
                request.groupByKeys() == null ? List.of() : request.groupByKeys(),
                request.fromEpochMs(), request.toEpochMs());
        return result.isComplete()
                ? ResponseEntity.ok(result)
                : ResponseEntity.unprocessableEntity().body(result);
    }

    /** 注册告警规则（含滞回阈值）。 */
    @PostMapping("/alerts/rules")
    public AlertRule registerRule(@RequestBody AlertRule rule) {
        return metricsService.registerRule(rule);
    }

    /** 查看全部规则在各序列上的当前状态（不消费事件）。 */
    @GetMapping("/alerts/status")
    public List<AlertStatusView> alertStatus() {
        return metricsService.alertStatuses();
    }

    /** 取走并清空累积的状态翻转事件。 */
    @PostMapping("/alerts/events/drain")
    public List<AlertEvent> drainEvents() {
        return metricsService.drainAlertEvents();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
