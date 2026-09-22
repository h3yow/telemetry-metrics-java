package com.github.h3yow.telemetrymetricsjava.alert;

import com.github.h3yow.telemetrymetricsjava.model.WindowStats;

/**
 * 规则可判定的聚合量。
 */
public enum MetricFunction {
    AVG {
        @Override
        public double extract(WindowStats s) {
            return s == null ? 0.0 : s.avg();
        }
    },
    SUM {
        @Override
        public double extract(WindowStats s) {
            return s == null ? 0.0 : s.sum();
        }
    },
    MAX {
        @Override
        public double extract(WindowStats s) {
            return s == null ? 0.0 : s.max();
        }
    },
    MIN {
        @Override
        public double extract(WindowStats s) {
            return s == null ? 0.0 : s.min();
        }
    },
    COUNT {
        @Override
        public double extract(WindowStats s) {
            return s == null ? 0.0 : s.count();
        }
    };

    public abstract double extract(WindowStats stats);
}
