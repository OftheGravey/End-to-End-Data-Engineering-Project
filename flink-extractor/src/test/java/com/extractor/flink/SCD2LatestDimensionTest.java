package com.extractor.flink;

import org.apache.commons.math3.optim.nonlinear.vector.Target;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;

import java.sql.Timestamp;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;

import static org.junit.jupiter.api.Assertions.*;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.SCD2MostValidFunction;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;

public class SCD2LatestDimensionTest {
    private static final long END_OF_TIME = 253402300799000L;
    private KeyedOneInputStreamOperatorTestHarness<String, TestDimension, TestDimension> testHarness;
    private TestSCD2ProcessFunction processFunction;

    private class TestDimension extends TargetDimensionRecord {
        String dimensionSk;
    }

    private static class TestSCD2ProcessFunction extends SCD2MostValidFunction<TestDimension> {
        public TestSCD2ProcessFunction() {
            super(TypeInformation.of(TestDimension.class));
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        processFunction = new TestSCD2ProcessFunction();

        // This value is for tests to make the values more human-readable
        processFunction.timeWindowSizeMs = 50L;

        KeyedProcessOperator<String, TestDimension, TestDimension> operator = new KeyedProcessOperator<>(
                processFunction);

        testHarness = new KeyedOneInputStreamOperatorTestHarness<String, TestDimension, TestDimension>(
                operator,
                record -> record.dimensionSk,
                TypeInformation.of(String.class));
        testHarness.open();
    }

    @Test
    public void testBasicOverwrite() throws Exception {
        // Record overwritten when closing record captured
        TestDimension record1 = new TestDimension();
        record1.dimensionSk = "A";
        record1.validFrom = 10L;
        record1.validTo = END_OF_TIME;
        TestDimension record2 = new TestDimension();
        record2.dimensionSk = "A";
        record2.validFrom = 10L;
        record2.validTo = 19L;

        testHarness.processElement(record1, 10L);
        testHarness.processElement(record2, 20L);

        testHarness.processWatermark(50L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(2, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((10L), outputRecord.validFrom);
        assertEquals(19L, outputRecord.validTo);
    }

    @Test
    public void testSimpleFlow() throws Exception {
        // Record fired after timer triggered
        TestDimension record1 = new TestDimension();
        record1.dimensionSk = "A";
        record1.validFrom = 10L;
        record1.validTo = END_OF_TIME;

        testHarness.processElement(record1, 10L);

        testHarness.processWatermark(50L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(2, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((10L), outputRecord.validFrom);
        assertEquals(END_OF_TIME, outputRecord.validTo);
    }

    @Test
    public void testLateOverwrite() throws Exception {
        // Record not overwritten because timer has already been triggered
        TestDimension record1 = new TestDimension();
        record1.dimensionSk = "A";
        record1.validFrom = 10L;
        record1.validTo = END_OF_TIME;
        TestDimension record2 = new TestDimension();
        record2.dimensionSk = "A";
        record2.validFrom = 10L;
        record2.validTo = 90L;

        testHarness.processElement(record1, 10L);
        testHarness.processWatermark(50L);
        testHarness.processElement(record2, 90L); // Outside of window
        testHarness.processWatermark(100L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(4, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((10L), outputRecord.validFrom);
        assertEquals(END_OF_TIME, outputRecord.validTo);

        // Dump watermark
        output.poll();

        TargetDimensionRecord outputRecord2 = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((10L), outputRecord2.validFrom);
        assertEquals(90L, outputRecord2.validTo);
    }

}