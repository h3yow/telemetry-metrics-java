package com.github.h3yow.telemetrymetricsjava.metrics.model;

/**
 * 分组查询或分组告警评估触及容量上界时抛出的明确异常。
 * 携带可区分的 {@link GroupLimitReason} 与配置上限/实际值，绝不静默截断或合并。
 */
public class GroupLimitException extends RuntimeException {

    private final GroupLimitReason reason;
    private final long limit;
    private final long actual;

    public GroupLimitException(GroupLimitReason reason, long limit, long actual, String message) {
        super(message);
        this.reason = reason;
        this.limit = limit;
        this.actual = actual;
    }

    public GroupLimitReason reason() {
        return reason;
    }

    public long limit() {
        return limit;
    }

    public long actual() {
        return actual;
    }
}
