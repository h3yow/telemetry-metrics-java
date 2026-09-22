package com.github.h3yow.telemetrymetricsjava.metrics;

import org.springframework.context.SmartLifecycle;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 真实运行时的窗口维护调度器：周期性触发封定（告警判定）与过期窗口淘汰。
 *
 * <p>测试中直接 new {@link MetricsService}（不经 Spring 生命周期）不会启动该线程，
 * 因此单测的窗口滚动完全由测试时钟 + {@link MetricsService#maintain()} 确定性驱动。</p>
 */
public class MetricsMaintenanceLifecycle implements SmartLifecycle {

    static final long TICK_MS = 500L;

    private final MetricsService metricsService;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> task;
    private volatile boolean running;

    public MetricsMaintenanceLifecycle(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-window-maintainer");
            t.setDaemon(true);
            return t;
        });
        task = executor.scheduleWithFixedDelay(this::safeMaintain,
                TICK_MS, TICK_MS, TimeUnit.MILLISECONDS);
        running = true;
    }

    private void safeMaintain() {
        try {
            metricsService.maintain();
        } catch (Throwable ignored) {
            // 维护线程不得因单次异常死亡。
        }
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (task != null) {
            task.cancel(false);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
