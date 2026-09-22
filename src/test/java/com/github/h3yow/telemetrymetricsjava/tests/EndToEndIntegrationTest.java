package com.github.h3yow.telemetrymetricsjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.telemetrymetricsjava.metrics.model.IngestResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端集成测试：经 HTTP 上报样本 -> 聚合查询 -> 注册规则 -> 查告警状态。
 * 全部本地运行，不依赖任何外部服务。
 */
@SpringBootTest
@AutoConfigureMockMvc
class EndToEndIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(EndToEndIntegrationTest.class);

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void ingestQueryAndAlertOverHttp() throws Exception {
        long now = System.currentTimeMillis();
        long windowStart = now - now % 10_000;

        List<Map<String, Object>> samples = List.of(
                Map.of("name", "req.latency", "value", 120.0, "timestamp", now - 100,
                        "labels", Map.of("api", "/login"), "sampleId", "e2e-1"),
                Map.of("name", "req.latency", "value", 140.0, "timestamp", now - 50,
                        "labels", Map.of("api", "/login"), "sampleId", "e2e-2"),
                Map.of("name", "req.latency", "value", -1.0, "timestamp", 0L,
                        "labels", Map.of("api", "/login"), "sampleId", "e2e-bad"));
        String body = objectMapper.writeValueAsString(samples);
        log.info("[E2E] POST /metrics/samples body={}", body);

        mockMvc.perform(post("/metrics/samples")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].accepted").value(true))
                .andExpect(jsonPath("$[0].windowStartMs").value(windowStart))
                .andExpect(jsonPath("$[2].accepted").value(false))
                .andExpect(jsonPath("$[2].rejectReason").value("TOO_OLD"));

        String query = objectMapper.writeValueAsString(Map.of(
                "name", "req.latency",
                "labels", Map.of("api", "/login"),
                "fromEpochMs", windowStart - 10_000,
                "toEpochMs", windowStart + 10_000));
        log.info("[E2E] POST /metrics/query body={}", query);
        mockMvc.perform(post("/metrics/query")
                        .contentType(MediaType.APPLICATION_JSON).content(query))
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].count").value(2))
                .andExpect(jsonPath("$[0].sum").value(260.0));

        String rule = objectMapper.writeValueAsString(Map.of(
                "ruleId", "latency-high", "metricName", "req.latency",
                "labelMatchers", Map.of("api", "/login"),
                "aggregator", "AVG",
                "fireThreshold", 100.0, "recoverThreshold", 80.0));
        log.info("[E2E] POST /metrics/alerts/rules body={}", rule);
        mockMvc.perform(post("/metrics/alerts/rules")
                        .contentType(MediaType.APPLICATION_JSON).content(rule))
                .andExpect(jsonPath("$.ruleId").value("latency-high"));

        // 状态接口可访问（当前窗口尚未封定，规则刚注册，无历史状态条目是正常的）
        mockMvc.perform(get("/metrics/alerts/status"))
                .andExpect(result -> log.info("[E2E] GET /metrics/alerts/status -> {}",
                        result.getResponse().getContentAsString()))
                .andExpect(jsonPath("$").isArray());

        // 非法请求返回 400 且原因可区分
        mockMvc.perform(post("/metrics/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("name", "x", "fromEpochMs", 10, "toEpochMs", 1))))
                .andExpect(result -> log.info("[E2E] 非法查询响应: {}",
                        result.getResponse().getContentAsString()))
                .andExpect(status().isBadRequest());
    }
}
