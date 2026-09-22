package com.github.h3yow.telemetrymetricsjava.web;

import com.github.h3yow.telemetrymetricsjava.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.alert.ComparisonOp;
import com.github.h3yow.telemetrymetricsjava.alert.MetricFunction;
import com.github.h3yow.telemetrymetricsjava.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.model.IngestStatus;
import com.github.h3yow.telemetrymetricsjava.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.service.TelemetryService;
import com.github.h3yow.telemetrymetricsjava.web.dto.IngestRequest;
import com.github.h3yow.telemetrymetricsjava.web.dto.RuleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 本地 HTTP 接口：样本上报、聚合查询、窗口推进、规则管理、告警查询。
 * 不依赖任何外部服务。
 */
@RestController
@RequestMapping("/telemetry")
public class TelemetryController {

    private static final Logger log = LoggerFactory.getLogger(TelemetryController.class);

    private final TelemetryService service;
    private final Clock clock;

    public TelemetryController(TelemetryService service, Clock clock) {
        this.service = service;
        this.clock = clock;
    }

    @PostMapping("/samples")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody IngestRequest req) {
        if (req.name() == null || req.name().isBlank() || req.value() == null) {
            throw new IllegalArgumentException("name and value are required");
        }
        long ts = req.timestampMs() != null ? req.timestampMs() : clock.millis();
        String sampleId = (req.sampleId() == null || req.sampleId().isBlank())
                ? UUID.randomUUID().toString() : req.sampleId();
        MetricSample sample = new MetricSample(req.name(), req.tags(), req.value(), ts, sampleId);
        log.info("ingest sample name={} tags={} value={} ts={} id={}",
                sample.name(), sample.tags(), sample.value(), ts, sampleId);
        IngestResult result = service.ingest(sample);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", result.status().name());
        body.put("windowStartMs", result.windowStartMs());
        body.put("reason", result.reason());
        HttpStatus http = switch (result.status()) {
            case ACCEPTED, DUPLICATE -> HttpStatus.OK;
            case REJECTED_WINDOW_FULL -> HttpStatus.TOO_MANY_REQUESTS;
            default -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        if (result.status() != IngestStatus.ACCEPTED) {
            log.warn("ingest rejected id={} status={} reason={}", sampleId, result.status(), result.reason());
        }
        return ResponseEntity.status(http).body(body);
    }

    @PostMapping("/tick")
    public Map<String, Object> tick(@RequestParam(required = false) Long nowMs) {
        long now = nowMs != null ? nowMs : clock.millis();
        List<AlertEvent> events = service.tick(now);
        return Map.of("nowMs", now, "watermarkMs", service.watermarkMs(), "events", events);
    }

    @GetMapping("/series/{name}/stats/latest")
    public ResponseEntity<Map<String, Object>> latest(@PathVariable String name,
                                                      @RequestParam(required = false) Map<String, String> tags) {
        SeriesKey key = new SeriesKey(name, tags);
        return service.latestStats(key)
                .map(s -> ResponseEntity.ok(toMap(s)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("status", "NO_DATA", "series", key.toString())));
    }

    @GetMapping("/alerts/status")
    public Map<String, Object> alertStatuses() {
        return Map.of("statuses", service.alertStatuses());
    }

    @GetMapping("/alerts/events")
    public Map<String, Object> events(@RequestParam(defaultValue = "50") int limit) {
        return Map.of("events", service.recentEvents(limit));
    }

    @PostMapping("/rules")
    public ResponseEntity<Map<String, Object>> createRule(@RequestBody RuleRequest req) {
        if (req.metricName() == null || req.function() == null || req.op() == null
                || req.threshold() == null) {
            throw new IllegalArgumentException("metricName, function, op, threshold are required");
        }
        String ruleId = (req.ruleId() == null || req.ruleId().isBlank())
                ? UUID.randomUUID().toString() : req.ruleId();
        AlertRule rule = new AlertRule(ruleId, req.metricName(), req.requiredTags(),
                MetricFunction.valueOf(req.function().trim().toUpperCase()),
                ComparisonOp.valueOf(req.op().trim().toUpperCase()), req.threshold());
        service.addRule(rule);
        log.info("rule added id={} metric={} {} {} {}", ruleId, req.metricName(),
                rule.function(), rule.op(), rule.threshold());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("rule", rule));
    }

    @GetMapping("/rules")
    public Map<String, Object> listRules() {
        return Map.of("rules", service.rules());
    }

    @DeleteMapping("/rules/{ruleId}")
    public Map<String, Object> deleteRule(@PathVariable String ruleId) {
        return Map.of("removed", service.removeRule(ruleId));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("status", "BAD_REQUEST", "reason", e.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> conflict(IllegalStateException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("status", "CONFLICT", "reason", e.getMessage()));
    }

    private static Map<String, Object> toMap(com.github.h3yow.telemetrymetricsjava.model.WindowStats s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("windowStartMs", s.windowStartMs());
        m.put("windowEndMs", s.windowEndMs());
        m.put("count", s.count());
        m.put("sum", s.sum());
        m.put("min", s.min());
        m.put("max", s.max());
        m.put("avg", s.avg());
        m.put("lastValue", s.lastValue());
        return m;
    }
}
