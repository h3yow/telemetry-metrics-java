package com.github.h3yow.telemetrymetricsjava.tests;

import com.github.h3yow.telemetrymetricsjava.model.WindowStats;
import com.github.h3yow.telemetrymetricsjava.window.TumblingWindow;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 单窗口累加器：基础聚合、幂等去重、容量拒绝、关闭后拒绝。
 */
class TumblingWindowTest {

    private static final Logger log = LoggerFactory.getLogger(TumblingWindowTest.class);

    @Test
    void aggregatesAndDeduplicates() {
        TumblingWindow w = new TumblingWindow(1_000L, 1_000L, 3);
        double[] inputs = {10, 20, 20, 30};
        String[] ids = {"a", "b", "b", "c"}; // b 重复
        for (int i = 0; i < inputs.length; i++) {
            var r = w.addSample(ids[i], inputs[i]);
            log.info("input id={} value={} -> {}", ids[i], inputs[i], r);
        }
        WindowStats s = w.snapshot();
        log.info("aggregated stats count={} sum={} min={} max={} avg={}",
                s.count(), s.sum(), s.min(), s.max(), s.avg());
        assertEquals(3, s.count());
        assertEquals(60.0, s.sum(), 1e-9);
        assertEquals(10.0, s.min(), 0);
        assertEquals(30.0, s.max(), 0);
        assertEquals(20.0, s.avg(), 1e-9);
    }

    @Test
    void capacityRejectionDoesNotOccupySlot() {
        TumblingWindow w = new TumblingWindow(0, 1_000L, 2);
        assertEquals(TumblingWindow.AddResult.ADDED, w.addSample("a", 1));
        assertEquals(TumblingWindow.AddResult.ADDED, w.addSample("b", 2));
        var rejected = w.addSample("c", 3);
        log.info("overflow sample c -> {}", rejected);
        assertEquals(TumblingWindow.AddResult.FULL, rejected);
        // 容量满时被拒绝的 id 不得占用槽位：挤出后新 id 可被识别为非重复，但这里容量仍满，
        // 关键是原统计保持不变
        assertEquals(2, w.sampleCount());
        assertEquals(TumblingWindow.AddResult.DUPLICATE, w.addSample("b", 9));
        assertEquals(2, w.sampleCount());
    }

    @Test
    void sealedWindowRejectsWritesAndReleasesDedup() {
        TumblingWindow w = new TumblingWindow(0, 1_000L, 10);
        w.addSample("a", 1);
        w.seal();
        assertTrue(w.isSealed());
        assertEquals(TumblingWindow.AddResult.SEALED, w.addSample("b", 2));
        assertFalse(w.containsSample("a"), "sealed window should release dedup set");
        assertNull(new TumblingWindow(0, 1_000L, 10).snapshot(), "empty window snapshot is null");
    }
}
