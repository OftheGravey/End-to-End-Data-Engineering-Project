package com.extractor.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;

import static org.junit.jupiter.api.Assertions.*;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;

public class SCD2ProcessFunctionTest {
    private static final long END_OF_TIME = 253402300799000L;
    private KeyedOneInputStreamOperatorTestHarness<Integer, DebeziumSourceRecord, TargetDimensionRecord> testHarness;
    private TestSCD2ProcessFunction processFunction;

    private static class TestSCD2ProcessFunction
            extends SCD2ProcessFunction<DebeziumSourceRecord, TargetDimensionRecord> {
        public TestSCD2ProcessFunction() {
            super(TypeInformation.of(DebeziumSourceRecord.class), TypeInformation.of(TargetDimensionRecord.class),
                    TargetDimensionRecord::new);
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        processFunction = new TestSCD2ProcessFunction();
        KeyedProcessOperator<Integer, DebeziumSourceRecord, TargetDimensionRecord> operator = new KeyedProcessOperator<>(
                processFunction);

        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                record -> 1,
                TypeInformation.of(Integer.class));
        testHarness.open();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    public void testSingleInsertRecord() throws Exception {
        DebeziumSourceRecord record = new DebeziumSourceRecord();
        record.tsMs = 1000L;
        record.op = "c"; // create/insert

        testHarness.processElement(record, 1000L);

        testHarness.setProcessingTime(12000L); // 1000 + 10000 + 1000 buffer

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((1000L), outputRecord.validFrom);
        assertEquals((END_OF_TIME), outputRecord.validTo);
    }

    @Test
    public void testMultipleRecordsInOrder() throws Exception {
        DebeziumSourceRecord record1 = new DebeziumSourceRecord();
        record1.tsMs = 1000L;
        record1.op = "c";

        DebeziumSourceRecord record2 = new DebeziumSourceRecord();
        record2.tsMs = 2000L;
        record2.op = "u"; // update

        DebeziumSourceRecord record3 = new DebeziumSourceRecord();
        record3.tsMs = 3000L;
        record3.op = "u";

        testHarness.processElement(record1, 1000L);
        testHarness.processElement(record2, 2000L);
        testHarness.processElement(record3, 3000L);

        testHarness.setProcessingTime(14000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(5, output.size());

        TargetDimensionRecord firstRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record1.tsMs), firstRecord.validFrom);
        assertEquals((END_OF_TIME), firstRecord.validTo);

        TargetDimensionRecord closedRecord1 = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record1.tsMs), closedRecord1.validFrom);
        assertEquals((record2.tsMs - 1), closedRecord1.validTo);

        TargetDimensionRecord secondRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record2.tsMs), secondRecord.validFrom);
        assertEquals((END_OF_TIME), secondRecord.validTo);

        TargetDimensionRecord closedRecord2 = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record2.tsMs), closedRecord2.validFrom);
        assertEquals((record3.tsMs - 1), closedRecord2.validTo);

        TargetDimensionRecord thirdRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record3.tsMs), thirdRecord.validFrom);
        assertEquals((END_OF_TIME), thirdRecord.validTo);
    }

    @Test
    public void testOutOfOrderRecords() throws Exception {
        DebeziumSourceRecord record1 = new DebeziumSourceRecord();
        record1.tsMs = 3000L;
        record1.op = "u";

        DebeziumSourceRecord record2 = new DebeziumSourceRecord();
        record2.tsMs = 1000L;
        record2.op = "c";

        DebeziumSourceRecord record3 = new DebeziumSourceRecord();
        record3.tsMs = 2000L;
        record3.op = "u";

        testHarness.processElement(record1, 3000L);
        testHarness.processElement(record2, 3100L);
        testHarness.processElement(record3, 3200L);

        testHarness.setProcessingTime(14000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(5, output.size());

        TargetDimensionRecord firstProcessed = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record2.tsMs), firstProcessed.validFrom);
        assertEquals((END_OF_TIME), firstProcessed.validTo);

        TargetDimensionRecord firstClosed = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record2.tsMs), firstClosed.validFrom);
        assertEquals((record3.tsMs - 1), firstClosed.validTo);

        TargetDimensionRecord secondProcessed = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record3.tsMs), secondProcessed.validFrom);
        assertEquals((END_OF_TIME), secondProcessed.validTo);

        TargetDimensionRecord secondClosed = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record3.tsMs), secondClosed.validFrom);
        assertEquals((record1.tsMs - 1), secondClosed.validTo);

        TargetDimensionRecord thirdProcessed = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record1.tsMs), thirdProcessed.validFrom);
        assertEquals((END_OF_TIME), thirdProcessed.validTo);
    }

    @Test
    public void testDeleteOperation() throws Exception {
        DebeziumSourceRecord insertRecord = new DebeziumSourceRecord();
        insertRecord.tsMs = 1000L;
        insertRecord.op = "c";

        DebeziumSourceRecord deleteRecord = new DebeziumSourceRecord();
        deleteRecord.tsMs = 2000L;
        deleteRecord.op = "d";

        testHarness.processElement(insertRecord, insertRecord.tsMs);
        testHarness.processElement(deleteRecord, deleteRecord.tsMs);

        testHarness.setProcessingTime(13000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());

        TargetDimensionRecord firstRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((insertRecord.tsMs), firstRecord.validFrom);
        assertEquals((END_OF_TIME), firstRecord.validTo);

        TargetDimensionRecord closedRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((insertRecord.tsMs), closedRecord.validFrom);
        assertEquals((deleteRecord.tsMs - 1), closedRecord.validTo);

        TargetDimensionRecord deleteRecordOutput = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((deleteRecord.tsMs), deleteRecordOutput.validFrom);
        assertEquals((END_OF_TIME), deleteRecordOutput.validTo); // END_OF_TIME
    }

    @Test
    public void testDeleteWithSubsequentRecord() throws Exception {
        DebeziumSourceRecord insertRecord = new DebeziumSourceRecord();
        insertRecord.tsMs = 1000L;
        insertRecord.op = "c";

        DebeziumSourceRecord deleteRecord = new DebeziumSourceRecord();
        deleteRecord.tsMs = 2000L;
        deleteRecord.op = "d";

        DebeziumSourceRecord insertRecord2 = new DebeziumSourceRecord();
        insertRecord2.tsMs = 3000L;
        insertRecord2.op = "c";

        testHarness.processElement(insertRecord, insertRecord.tsMs);
        testHarness.processElement(deleteRecord, deleteRecord.tsMs);
        testHarness.processElement(insertRecord2, insertRecord2.tsMs);

        testHarness.setProcessingTime(14000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(4, output.size());

        output.poll(); // first insert
        output.poll(); // closed first insert
        TargetDimensionRecord deleteRecordOutput = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((deleteRecord.tsMs), deleteRecordOutput.validFrom);
        assertEquals((END_OF_TIME), deleteRecordOutput.validTo); // Next record's tsMs + 1
    }

    @Test
    public void testBufferTimeout() throws Exception {
        DebeziumSourceRecord record = new DebeziumSourceRecord();
        record.tsMs = 1000L;
        record.op = "c";

        testHarness.setProcessingTime(5000L);
        testHarness.processElement(record, 5000L);

        testHarness.setProcessingTime(14999L);
        assertTrue(testHarness.getOutput().isEmpty());

        testHarness.setProcessingTime(15000L);
        assertFalse(testHarness.getOutput().isEmpty());
        assertEquals(1, testHarness.getOutput().size());
    }

    @Test
    public void testEmptyState() throws Exception {
        DebeziumSourceRecord record = new DebeziumSourceRecord();
        record.tsMs = 1000L;
        record.op = "u";

        testHarness.processElement(record, 1000L);
        testHarness.setProcessingTime(12000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals((record.tsMs), outputRecord.validFrom);
        assertEquals((END_OF_TIME), outputRecord.validTo);
    }
}