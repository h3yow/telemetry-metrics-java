package com.github.h3yow.telemetrymetricsjava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 周期性推进窗口水位的后台任务。仅在 {@code telemetry.auto-tick-enabled=true}（默认）
 * 时由配置装配；测试关闭它，通过 {@code POST /telemetry/tick} 按显式时间确定性驱动。
 *
 * <p>用 AtomicBoolean 防止任务与自身重入重叠；与 ingest 的互斥由 WindowStore 保证。</p>
 */
public class WindowTicker {

    private static final Logger log = LoggerFactory.getLogger(WindowTicker.class);

    private final TelemetryService service;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public WindowTicker(TelemetryService service) {
        this.service = service;
    }

    @Scheduled(fixedDelayString = "${telemetry.tick-interval-ms:1000}",
            initialDelayString = "${telemetry.tick-interval-ms:1000}")
    public void tick() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            var events = service.tick();
            for (var e : events) {
                log.info("alert {} rule={} series={} windowStart={} observed={} basis={}",
                        e.kind(), e.ruleId(), e.series(), e.windowStartMs(), e.observedValue(),
                        e.thresholdMessage());
            }
        } finally {
            running.set(false);
        }
    }
}
