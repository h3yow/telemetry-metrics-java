package com.github.h3yow.telemetrymetricsjava.metrics;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MetricsProperties.class)
public class MetricsConfig {

    @Bean
    public MetricsService metricsService(MetricsProperties properties) {
        return new MetricsService(properties);
    }

    @Bean
    public MetricsMaintenanceLifecycle metricsMaintenanceLifecycle(MetricsService service) {
        return new MetricsMaintenanceLifecycle(service);
    }
}
