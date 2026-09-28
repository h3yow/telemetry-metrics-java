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

    @Test
    void groupedQueryOverHttpMergesByLabelAndSignalsLimits() throws Exception {
        long now = System.currentTimeMillis();
        long windowStart = now - now % 10_000;

        // 同一 host、不同 service 的两条序列 -> 按 host 分组合并。
        List<Map<String, Object>> samples = List.of(
                Map.of("name", "cpu", "value", 10.0, "timestamp", now - 100,
                        "labels", Map.of("host", "a", "service", "x"), "sampleId", "g1"),
                Map.of("name", "cpu", "value", 20.0, "timestamp", now - 50,
                        "labels", Map.of("host", "a", "service", "y"), "sampleId", "g2"),
                Map.of("name", "cpu", "value", 70.0, "timestamp", now - 50,
                        "labels", Map.of("host", "b"), "sampleId", "g3"));
        mockMvc.perform(post("/metrics/samples")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(samples)))
                .andExpect(jsonPath("$.length()").value(3));

        String gq = objectMapper.writeValueAsString(Map.of(
                "name", "cpu",
                "groupByKeys", List.of("host"),
                "fromEpochMs", windowStart,
                "toEpochMs", windowStart));
        log.info("[E2E] POST /metrics/query-groups body={}", gq);
        mockMvc.perform(post("/metrics/query-groups")
                        .contentType(MediaType.APPLICATION_JSON).content(gq))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK"))
                .andExpect(jsonPath("$.results.length()").value(2))
                // 结果按分组标签字典序：host=a 在前，跨两条序列合并 sampleCount=2/seriesCount=2。
                .andExpect(jsonPath("$.results[0].groupLabels.host").value("a"))
                .andExpect(jsonPath("$.results[0].sampleCount").value(2))
                .andExpect(jsonPath("$.results[0].seriesCount").value(2))
                .andExpect(jsonPath("$.results[0].sum").value(30.0))
                .andExpect(jsonPath("$.results[1].groupLabels.host").value("b"));

        // 请求范围超过 maxQueryWindows 默认上限 -> 422 + 可区分状态，不静默截断。
        String tooWide = objectMapper.writeValueAsString(Map.of(
                "name", "cpu",
                "groupByKeys", List.of("host"),
                "fromEpochMs", windowStart - 10_000_000L,
                "toEpochMs", windowStart));
        mockMvc.perform(post("/metrics/query-groups")
                        .contentType(MediaType.APPLICATION_JSON).content(tooWide))
                .andExpect(result -> log.info("[E2E] 超宽分组查询响应: {}",
                        result.getResponse().getContentAsString()))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status").value("TOO_MANY_WINDOWS"))
                .andExpect(jsonPath("$.results.length()").value(0));
    }
}
