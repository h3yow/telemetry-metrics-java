package com.github.h3yow.telemetrymetricsjava.service;

import com.github.h3yow.telemetrymetricsjava.alert.AlertEngine;
import com.github.h3yow.telemetrymetricsjava.alert.AlertEvent;
import com.github.h3yow.telemetrymetricsjava.alert.AlertRule;
import com.github.h3yow.telemetrymetricsjava.alert.AlertStatusView;
import com.github.h3yow.telemetrymetricsjava.config.TelemetryProperties;
import com.github.h3yow.telemetrymetricsjava.model.IngestResult;
import com.github.h3yow.telemetrymetricsjava.model.MetricSample;
import com.github.h3yow.telemetrymetricsjava.model.SeriesKey;
import com.github.h3yow.telemetrymetricsjava.model.WindowStats;
import com.github.h3yow.telemetrymetricsjava.window.WindowStore;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * 对外门面：样本上报、窗口推进（滚动）、聚合查询、规则管理、告警状态与事件查询。
 *
 * <p>窗口滚动与告警判定在同一个 {@link #tick(long)} 调用内串行完成：
 * 先由 WindowStore 关闭窗口（每个窗口恰好一次），再逐条交给 AlertEngine 判定，
 * 因此“窗口切换”和“告警结论”在并发上报下保持一致。</p>
 */
public class TelemetryService {

    private final WindowStore store;
    private final AlertEngine engine;
    private final Clock clock;
    private final int eventHistorySize;

    /** 有界告警事件历史：最近的在前，超出容量淘汰最旧 */
    private final Deque<AlertEvent> eventHistory = new ArrayDeque<>();
    private final Object eventLock = new Object();

    public TelemetryService(WindowStore store, AlertEngine engine, Clock clock, TelemetryProperties props) {
        this.store = store;
        this.engine = engine;
        this.clock = clock;
        this.eventHistorySize = props.getEventHistorySize();
    }

    public IngestResult ingest(MetricSample sample) {
        return store.ingest(sample);
    }

    /** 用调用方给定的“现在”推进窗口（测试可确定性驱动；生产由定时任务/接口触发）。 */
    public List<AlertEvent> tick(long nowMs) {
        List<WindowStore.SealedWindow> sealed = store.advanceWatermark(nowMs);
        List<AlertEvent> produced = new ArrayList<>();
        for (WindowStore.SealedWindow sw : sealed) {
            produced.addAll(engine.evaluateWindow(sw.key(), sw.stats()));
        }
        if (!produced.isEmpty()) {
            synchronized (eventLock) {
                for (AlertEvent e : produced) {
                    eventHistory.addFirst(e);
                }
                while (eventHistory.size() > eventHistorySize) {
                    eventHistory.removeLast();
                }
            }
        }
        return produced;
    }

    /** 以注入时钟的当前时间推进。 */
    public List<AlertEvent> tick() {
        return tick(clock.millis());
    }

    public Optional<WindowStats> latestStats(SeriesKey key) {
        return store.latestStats(key);
    }

    public List<WindowStats> statsBetween(SeriesKey key, long fromWindowStart, long toWindowStart) {
        return store.statsBetween(key, fromWindowStart, toWindowStart);
    }

    public void addRule(AlertRule rule) {
        engine.addRule(rule);
    }

    public List<AlertRule> rules() {
        return engine.rules();
    }

    public boolean removeRule(String ruleId) {
        return engine.removeRule(ruleId);
    }

    public List<AlertStatusView> alertStatuses() {
        return engine.currentStatuses();
    }

    public List<AlertEvent> recentEvents(int limit) {
        synchronized (eventLock) {
            int n = Math.max(0, Math.min(limit, eventHistory.size()));
            List<AlertEvent> out = new ArrayList<>(n);
            int i = 0;
            for (AlertEvent e : eventHistory) {
                if (i++ >= n) {
                    break;
                }
                out.add(e);
            }
            return out;
        }
    }

    public int seriesCount() {
        return store.seriesCount();
    }

    public long watermarkMs() {
        return store.watermarkMs();
    }
}
