package cn.edu.zju.daily.function.partitioner.sax;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HotTracerTest {

    @Test
    void test() {
        HotTracer hotTracer = new HotTracer(10, 3);
        hotTracer.add(1);
        hotTracer.add(2);
        hotTracer.add(3);

        assertEquals(3, hotTracer.getHotKeys().size());
        assertTrue(hotTracer.isHot(1));
        assertTrue(hotTracer.isHot(2));
        assertTrue(hotTracer.isHot(3));

        hotTracer.add(4);
        hotTracer.add(5);
        hotTracer.add(2);
        hotTracer.add(4);
        hotTracer.add(1);

        assertEquals(3, hotTracer.getHotKeys().size());
        assertTrue(hotTracer.isHot(2));
        assertTrue(hotTracer.isHot(4));
        assertTrue(hotTracer.isHot(1));

        hotTracer.add(1);
        hotTracer.add(2);
        hotTracer.add(4);
        hotTracer.add(4);

        assertEquals(4, (int) hotTracer.getHotList().getLast().getLeft());

        hotTracer.clear();
        assertEquals(0, hotTracer.getHotKeys().size());
    }
}
