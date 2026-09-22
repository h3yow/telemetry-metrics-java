package com.github.h3yow.telemetrymetricsjava.model;

/**
 * 单次上报的处理结果。{@code windowStartMs} 为样本归入的窗口起点（被拒绝时为 -1）。
 */
public record IngestResult(IngestStatus status, long windowStartMs, String reason) {

    public static IngestResult accepted(long windowStartMs) {
        return new IngestResult(IngestStatus.ACCEPTED, windowStartMs, "accepted");
    }

    public static IngestResult duplicate(long windowStartMs) {
        return new IngestResult(IngestStatus.DUPLICATE, windowStartMs, "duplicate sampleId ignored");
    }

    public static IngestResult rejected(IngestStatus status, String reason) {
        return new IngestResult(status, -1, reason);
    }
}
