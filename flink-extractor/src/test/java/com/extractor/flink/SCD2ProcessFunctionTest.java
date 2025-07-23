package com.extractor.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.sql.Timestamp;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;

public class SCD2ProcessFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<Integer, DebeziumSourceRecord, TargetDimensionRecord> testHarness;
    private TestSCD2ProcessFunction processFunction;

    // Concrete implementation for testing
    private static class TestSCD2ProcessFunction extends SCD2ProcessFunction<DebeziumSourceRecord, TargetDimensionRecord> {
        public TestSCD2ProcessFunction() {
            super(TypeInformation.of(DebeziumSourceRecord.class), TypeInformation.of(TargetDimensionRecord.class), TargetDimensionRecord::new);
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        processFunction = new TestSCD2ProcessFunction();
        
        // Create the keyed process operator with explicit types
        org.apache.flink.streaming.api.operators.KeyedProcessOperator<Integer, DebeziumSourceRecord, TargetDimensionRecord> operator = 
            new org.apache.flink.streaming.api.operators.KeyedProcessOperator<>(processFunction);
        
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                record -> 1, // Key selector - all records go to same key for simplicity
                TypeInformation.of(Integer.class)
        );
        testHarness.open();
    }

    
    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    public void testSingleInsertRecord() throws Exception {
        // Create a single insert record
        DebeziumSourceRecord record = new DebeziumSourceRecord();
        record.tsMs = 1000L;
        record.op = "c"; // create/insert

        // Process the record
        testHarness.processElement(record, 1000L);
        
        // Advance time to trigger timer
        testHarness.setProcessingTime(12000L); // 1000 + 10000 + 1000 buffer

        // Verify output
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), outputRecord.validFrom);
        assertEquals(new Timestamp(1001L), outputRecord.validTo);
    }

    @Test
    public void testMultipleRecordsInOrder() throws Exception {
        // Create multiple records in chronological order
        DebeziumSourceRecord record1 = new DebeziumSourceRecord();
        record1.tsMs = 1000L;
        record1.op = "c";

        DebeziumSourceRecord record2 = new DebeziumSourceRecord();
        record2.tsMs = 2000L;
        record2.op = "u"; // update

        DebeziumSourceRecord record3 = new DebeziumSourceRecord();
        record3.tsMs = 3000L;
        record3.op = "u";

        // Process records
        testHarness.processElement(record1, 1000L);
        testHarness.processElement(record2, 2000L);
        testHarness.processElement(record3, 3000L);

        // Advance time to trigger timer
        testHarness.setProcessingTime(14000L);

        // Verify output - should have 5 records total:
        // 1. First record (1000L)
        // 2. Close first record when second arrives
        // 3. Second record (2000L) 
        // 4. Close second record when third arrives
        // 5. Third record (3000L)
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(5, output.size());

        // First record
        TargetDimensionRecord firstRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), firstRecord.validFrom);
        assertEquals(new Timestamp(1001L), firstRecord.validTo);

        // First record gets closed when second arrives
        TargetDimensionRecord closedRecord1 = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), closedRecord1.validFrom);
        assertEquals(new Timestamp(2000L), closedRecord1.validTo);

        // Second record
        TargetDimensionRecord secondRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(2000L), secondRecord.validFrom);
        assertEquals(new Timestamp(2001L), secondRecord.validTo);

        // Second record gets closed when third arrives
        TargetDimensionRecord closedRecord2 = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(2000L), closedRecord2.validFrom);
        assertEquals(new Timestamp(3000L), closedRecord2.validTo);

        // Third record
        TargetDimensionRecord thirdRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(3000L), thirdRecord.validFrom);
        assertEquals(new Timestamp(3001L), thirdRecord.validTo);
    }

    @Test
    public void testOutOfOrderRecords() throws Exception {
        // Create records that arrive out of chronological order
        DebeziumSourceRecord record1 = new DebeziumSourceRecord();
        record1.tsMs = 3000L;
        record1.op = "u";

        DebeziumSourceRecord record2 = new DebeziumSourceRecord();
        record2.tsMs = 1000L;
        record2.op = "c";

        DebeziumSourceRecord record3 = new DebeziumSourceRecord();
        record3.tsMs = 2000L;
        record3.op = "u";

        // Process records in wrong order
        testHarness.processElement(record1, 3000L);
        testHarness.processElement(record2, 3100L);
        testHarness.processElement(record3, 3200L);

        // Advance time to trigger timer
        testHarness.setProcessingTime(14000L);

        // Verify output - records should be processed in chronological order (1000, 2000, 3000)
        // Expected: 5 records total
        // 1. First record (1000) - no previous to close
        // 2. Close first record when second arrives 
        // 3. Second record (2000)
        // 4. Close second record when third arrives
        // 5. Third record (3000)
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(5, output.size());

        // First record (1000L) - no previous record to close
        TargetDimensionRecord firstProcessed = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), firstProcessed.validFrom);
        assertEquals(new Timestamp(1001L), firstProcessed.validTo);

        // First record gets closed when second arrives (validTo becomes 2000L)
        TargetDimensionRecord firstClosed = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), firstClosed.validFrom);
        assertEquals(new Timestamp(2000L), firstClosed.validTo);

        // Second record (2000L)
        TargetDimensionRecord secondProcessed = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(2000L), secondProcessed.validFrom);
        assertEquals(new Timestamp(2001L), secondProcessed.validTo);

        // Second record gets closed when third arrives (validTo becomes 3000L)
        TargetDimensionRecord secondClosed = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(2000L), secondClosed.validFrom);
        assertEquals(new Timestamp(3000L), secondClosed.validTo);

        // Third record (3000L)
        TargetDimensionRecord thirdProcessed = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(3000L), thirdProcessed.validFrom);
        assertEquals(new Timestamp(3001L), thirdProcessed.validTo);
    }

    @Test
    public void testDeleteOperation() throws Exception {
        // Create insert and then delete
        DebeziumSourceRecord insertRecord = new DebeziumSourceRecord();
        insertRecord.tsMs = 1000L;
        insertRecord.op = "c";

        DebeziumSourceRecord deleteRecord = new DebeziumSourceRecord();
        deleteRecord.tsMs = 2000L;
        deleteRecord.op = "d"; // delete

        // Process records
        testHarness.processElement(insertRecord, 1000L);
        testHarness.processElement(deleteRecord, 2000L);

        // Advance time to trigger timer
        testHarness.setProcessingTime(13000L);

        // Verify output
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());

        // First record
        TargetDimensionRecord firstRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), firstRecord.validFrom);
        assertEquals(new Timestamp(1001L), firstRecord.validTo);

        // Insert record gets closed by delete
        TargetDimensionRecord closedRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), closedRecord.validFrom);
        assertEquals(new Timestamp(2000L), closedRecord.validTo);

        // Delete record - should have END_OF_TIME as validTo since it's a delete with no following record
        TargetDimensionRecord deleteRecordOutput = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(2000L), deleteRecordOutput.validFrom);
        assertEquals(new Timestamp(253402300799000L), deleteRecordOutput.validTo); // END_OF_TIME
    }

    @Test
    public void testDeleteWithSubsequentRecord() throws Exception {
        // Create insert, delete, then another insert
        DebeziumSourceRecord insertRecord = new DebeziumSourceRecord();
        insertRecord.tsMs = 1000L;
        insertRecord.op = "c";

        DebeziumSourceRecord deleteRecord = new DebeziumSourceRecord();
        deleteRecord.tsMs = 2000L;
        deleteRecord.op = "d";

        DebeziumSourceRecord insertRecord2 = new DebeziumSourceRecord();
        insertRecord2.tsMs = 3000L;
        insertRecord2.op = "c";

        // Process records
        testHarness.processElement(insertRecord, 1000L);
        testHarness.processElement(deleteRecord, 2000L);
        testHarness.processElement(insertRecord2, 3000L);

        // Advance time to trigger timer
        testHarness.setProcessingTime(14000L);

        // Verify output
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(4, output.size());

        // Skip to delete record - it should have validTo = next record's tsMs + 1
        output.poll(); // first insert
        output.poll(); // closed first insert
        TargetDimensionRecord deleteRecordOutput = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(2000L), deleteRecordOutput.validFrom);
        assertEquals(new Timestamp(3001L), deleteRecordOutput.validTo); // Next record's tsMs + 1
    }

    @Test
    public void testBufferTimeout() throws Exception {
        // Test that timer correctly triggers after buffer timeout
        DebeziumSourceRecord record = new DebeziumSourceRecord();
        record.tsMs = 1000L;
        record.op = "c";

        // Process record at time 5000
        testHarness.setProcessingTime(5000L);
        testHarness.processElement(record, 5000L);

        // Advance time to just before timeout (5000 + 10000 = 15000)
        testHarness.setProcessingTime(14999L);
        assertTrue(testHarness.getOutput().isEmpty());

        // Advance time to trigger timeout
        testHarness.setProcessingTime(15000L);
        assertFalse(testHarness.getOutput().isEmpty());
        assertEquals(1, testHarness.getOutput().size());
    }

    @Test
    public void testEmptyState() throws Exception {
        // Test processing when there's no current record in state
        DebeziumSourceRecord record = new DebeziumSourceRecord();
        record.tsMs = 1000L;
        record.op = "u"; // update on empty state

        testHarness.processElement(record, 1000L);
        testHarness.setProcessingTime(12000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());

        TargetDimensionRecord outputRecord = (TargetDimensionRecord) ((org.apache.flink.streaming.runtime.streamrecord.StreamRecord<?>) output.poll()).getValue();
        assertEquals(new Timestamp(1000L), outputRecord.validFrom);
        assertEquals(new Timestamp(1001L), outputRecord.validTo);
    }
}