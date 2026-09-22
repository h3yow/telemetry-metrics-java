package com.github.h3yow.telemetrymetricsjava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：HTTP 上报 -> 滚动 -> 告警 -> 查询，以及可区分的拒绝原因。
 * 使用显式事件时间与显式 tick 时间，保证结果可复现，不依赖真实墙钟。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"telemetry.window-size-ms=1000", "telemetry.allowed-lateness-ms=0",
                "telemetry.window-capacity=2", "telemetry.auto-tick-enabled=false"})
class TelemetryHttpE2ETest {

    private static final Logger log = LoggerFactory.getLogger(TelemetryHttpE2ETest.class);
    private static final long T0 = 1_700_000_000_000L;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private String url(String path) {
        return "http://localhost:" + port + "/telemetry" + path;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> body(ResponseEntity<Map> r) {
        return r.getBody();
    }

    @Test
    void fullLifecycleOverHttp() {
        // 1) 创建规则：cpu(host=web1) avg > 10
        Map<String, Object> rule = new LinkedHashMap<>();
        rule.put("ruleId", "hot-cpu");
        rule.put("metricName", "cpu");
        rule.put("requiredTags", Map.of("host", "web1"));
        rule.put("function", "avg");
        rule.put("op", "gt");
        rule.put("threshold", 10.0);
        var ruleResp = rest.postForEntity(url("/rules"), new HttpEntity<>(rule), Map.class);
        log.info("create rule -> {} {}", ruleResp.getStatusCode(), ruleResp.getBody());
        assertEquals(HttpStatus.CREATED, ruleResp.getStatusCode());

        // 2) 上报越线样本（显式事件时间）
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("name", "cpu");
        sample.put("tags", Map.of("host", "web1"));
        sample.put("value", 42);
        sample.put("timestampMs", T0 + 100);
        sample.put("sampleId", "e2e-1");
        var ing1 = rest.postForEntity(url("/samples"), new HttpEntity<>(sample), Map.class);
        log.info("ingest sample -> {}", ing1.getBody());
        assertEquals(HttpStatus.OK, ing1.getStatusCode());
        assertEquals("ACCEPTED", body(ing1).get("status"));

        // 幂等重放
        var ingDup = rest.postForEntity(url("/samples"), new HttpEntity<>(sample), Map.class);
        log.info("replay sample -> {}", ingDup.getBody());
        assertEquals("DUPLICATE", body(ingDup).get("status"));

        // 容量拒绝可区分（窗口容量=2）
        sample.put("sampleId", "e2e-2");
        rest.postForEntity(url("/samples"), new HttpEntity<>(sample), Map.class);
        sample.put("sampleId", "e2e-3");
        var full = rest.postForEntity(url("/samples"), new HttpEntity<>(sample), Map.class);
        log.info("capacity overflow -> status={} body={}", full.getStatusCode(), full.getBody());
        assertEquals(HttpStatus.TOO_MANY_REQUESTS, full.getStatusCode());
        assertEquals("REJECTED_WINDOW_FULL", body(full).get("status"));

        // 3) 显式滚动：now = T0+1000 关闭 [T0,T0+1000)
        var tick = rest.postForEntity(url("/tick?nowMs=" + (T0 + 1_000)), null, Map.class);
        log.info("tick -> {}", tick.getBody());
        var events = (java.util.List<?>) body(tick).get("events");
        assertEquals(1, events.size());

        // 4) 迟到样本被明确拒绝且原因可区分
        sample.put("sampleId", "e2e-late");
        sample.put("value", 1);
        var late = rest.postForEntity(url("/samples"), new HttpEntity<>(sample), Map.class);
        log.info("late sample -> status={} body={}", late.getStatusCode(), late.getBody());
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, late.getStatusCode());
        assertEquals("REJECTED_TOO_OLD", body(late).get("status"));

        // 5) 查询告警状态与事件
        var status = rest.getForEntity(url("/alerts/status"), Map.class);
        log.info("alert statuses -> {}", status.getBody());
        var statuses = (java.util.List<?>) body(status).get("statuses");
        assertEquals(1, statuses.size());
        assertTrue(statuses.get(0).toString().contains("FIRING"));

        var ev = rest.getForEntity(url("/alerts/events?limit=10"), Map.class);
        log.info("events -> {}", ev.getBody());
        assertEquals(1, ((java.util.List<?>) body(ev).get("events")).size());

        // 6) 聚合查询：avg=42（重复样本未重复计数，被容量拒绝的样本未计入）
        var stats = rest.getForEntity(url("/series/cpu/stats/latest?host=web1"), Map.class);
        log.info("latest stats -> {}", stats.getBody());
        assertEquals(2, ((Number) body(stats).get("count")).intValue());
        assertEquals(42.0, ((Number) body(stats).get("avg")).doubleValue(), 1e-9);

        // 7) 规则列表与删除
        var rules = rest.getForEntity(url("/rules"), Map.class);
        assertEquals(1, ((java.util.List<?>) body(rules).get("rules")).size());
        rest.delete(url("/rules/hot-cpu"));
        var rulesAfter = rest.getForEntity(url("/rules"), Map.class);
        assertEquals(0, ((java.util.List<?>) body(rulesAfter).get("rules")).size());
    }
}
