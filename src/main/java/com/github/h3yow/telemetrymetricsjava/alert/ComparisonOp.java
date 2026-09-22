package com.github.h3yow.telemetrymetricsjava.alert;

/** 阈值比较方向。 */
public enum ComparisonOp {
    GT, GTE, LT, LTE;

    public boolean test(double actual, double threshold) {
        return switch (this) {
            case GT -> actual > threshold;
            case GTE -> actual >= threshold;
            case LT -> actual < threshold;
            case LTE -> actual <= threshold;
        };
    }
}
