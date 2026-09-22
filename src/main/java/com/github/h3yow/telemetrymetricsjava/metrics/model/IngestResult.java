package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 单条样本上报结果。
 *
 * @param accepted      是否被接受
 * @param rejectReason  被拒原因（accepted=true 时为 null）
 * @param windowStartMs 归入窗口的起始时间（被拒时为 -1）
 * @param detail        可解释的判定说明
 */
public record IngestResult(boolean accepted, RejectReason rejectReason,
                           long windowStartMs, String detail) {

    public static IngestResult accepted(long windowStartMs, String detail) {
        return new IngestResult(true, null, windowStartMs, detail);
    }

    public static IngestResult rejected(RejectReason reason, String detail) {
        return new IngestResult(false, reason, -1L, detail);
    }
}
