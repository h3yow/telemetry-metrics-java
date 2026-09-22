package com.github.h3yow.telemetrymetricsjava.web.dto;

import java.util.Map;

/** 样本上报请求体。timestampMs 缺省时由服务端按当前时间填充。 */
public record IngestRequest(String name, Map<String, String> tags, Double value, Long timestampMs,
                            String sampleId) {
}
