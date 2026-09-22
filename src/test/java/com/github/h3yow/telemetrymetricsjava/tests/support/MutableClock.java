package com.github.h3yow.telemetrymetricsjava.tests.support;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * 测试用可控时钟：时间只能由测试显式推进，保证窗口滚动/迟到判定在单测中完全确定、可复现。
 */
public class MutableClock extends Clock {

    private volatile long nowMs;

    public MutableClock(long startMs) {
        this.nowMs = startMs;
    }

    public void setTime(long ms) {
        this.nowMs = ms;
    }

    public void advanceMs(long delta) {
        this.nowMs += delta;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(nowMs);
    }

    @Override
    public long millis() {
        return nowMs;
    }
}
