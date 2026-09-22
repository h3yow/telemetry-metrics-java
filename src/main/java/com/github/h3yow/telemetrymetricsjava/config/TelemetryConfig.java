package com.github.h3yow.telemetrymetricsjava.config;

import com.github.h3yow.telemetrymetricsjava.alert.AlertEngine;
import com.github.h3yow.telemetrymetricsjava.service.TelemetryService;
import com.github.h3yow.telemetrymetricsjava.window.WindowStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * 引擎装配：时钟、窗口存储、告警引擎、服务门面。
 */
@Configuration
@EnableConfigurationProperties(TelemetryProperties.class)
public class TelemetryConfig {

    @Bean
    public Clock systemClock() {
        return Clock.systemUTC();
    }

    @Bean
    public WindowStore windowStore(TelemetryProperties p, Clock clock) {
        return new WindowStore(p.getWindowSizeMs(), p.getWindowCapacity(), p.getRetainedWindowsPerSeries(),
                p.getMaxSeries(), p.getMaxFutureSkewMs(), p.getAllowedLatenessMs(), clock);
    }

    @Bean
    public AlertEngine alertEngine(WindowStore store) {
        return new AlertEngine(store);
    }

    @Bean
    public TelemetryService telemetryService(WindowStore store, AlertEngine engine, Clock clock,
                                             TelemetryProperties p) {
        return new TelemetryService(store, engine, clock, p);
    }

    /**
     * 自动滚动任务：默认启用；telemetry.auto-tick-enabled=false 时不装配
     * （测试用显式 tick 时间驱动，避免真实墙钟使历史样本被判为 TOO_OLD）。
     */
    @org.springframework.context.annotation.Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
            name = "telemetry.auto-tick-enabled", havingValue = "true", matchIfMissing = true)
    public com.github.h3yow.telemetrymetricsjava.service.WindowTicker windowTicker(TelemetryService service) {
        return new com.github.h3yow.telemetrymetricsjava.service.WindowTicker(service);
    }
}
