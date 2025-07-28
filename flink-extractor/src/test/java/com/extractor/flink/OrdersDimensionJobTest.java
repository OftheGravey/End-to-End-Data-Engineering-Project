package com.extractor.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;

public class OrdersDimensionJobTest extends OrdersDimensionJob {
    private KeyedOneInputStreamOperatorTestHarness<Integer, Order, OrderDimension> testHarness;
    private OrdersSCD2ProcessFunction processFunction;
    private static final long END_OF_TIME = 253402300799000L;

    @BeforeEach
    public void setup() throws Exception {
        processFunction = new OrdersSCD2ProcessFunction();

        // Create the keyed process operator with explicit types
        KeyedProcessOperator<Integer, Order, OrderDimension> operator = new KeyedProcessOperator<>(
                processFunction);

        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                record -> record.orderId, // Key selector - all records go to same key for simplicity
                TypeInformation.of(Integer.class));
        testHarness.open();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    public void testKeyedStream() throws Exception {
        // Standard un-important values
        String shippingMethod = "IGNORE";
        Long orderDate = 0L;

        // Record 1
        Order record1 = new Order();
        record1.orderId = 1;
        record1.tsMs = 1000L;
        record1.op = "c";
        record1.status = "pending";
        record1.shippingMethod = shippingMethod;
        record1.orderDate = orderDate;
        // Record 2
        Order record2 = new Order();
        record2.orderId = 1;
        record2.tsMs = 2000L;
        record2.op = "u";
        record2.status = "shipped";
        record2.shippingMethod = shippingMethod;
        record2.orderDate = orderDate;
        // Record 3
        Order record3 = new Order();
        record3.orderId = 2;
        record3.tsMs = 3000L;
        record3.op = "c";
        record3.status = "pending";
        record3.shippingMethod = shippingMethod;
        record3.orderDate = orderDate;

        // Process the record
        testHarness.processElement(record1, record1.tsMs);
        testHarness.processElement(record2, record2.tsMs);
        testHarness.processElement(record3, record3.tsMs);
        
        // Advance time to trigger timer
        testHarness.setProcessingTime(10000L);

        // Verify output
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(4, output.size());

        // Order 1 - record 1 created
        OrderDimension firstOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        System.out.println("Record 1");
        System.out.println(firstOutputRecord);
        System.out.println(firstOutputRecord.validTo.getTime());
        assertEquals(new Timestamp(record1.tsMs), firstOutputRecord.validFrom);
        assertEquals(new Timestamp(END_OF_TIME), firstOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 closed
        OrderDimension secondOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        System.out.println("Record 2");
        System.out.println(secondOutputRecord);
        System.out.println(secondOutputRecord.validTo.getTime());
        assertEquals(record1.tsMs, secondOutputRecord.validFrom.getTime());
        assertEquals(record2.tsMs - 1, secondOutputRecord.validTo.getTime());
        assertEquals(record1.status, firstOutputRecord.status);
        
        // Order 1 - record 2 updated
        OrderDimension fourthOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        System.out.println(fourthOutputRecord.validTo.getTime());
        System.out.println(fourthOutputRecord);
        assertEquals(new Timestamp(record2.tsMs), fourthOutputRecord.validFrom);
        assertEquals(new Timestamp(END_OF_TIME), fourthOutputRecord.validTo);
        assertEquals(record2.status, fourthOutputRecord.status);
        
        // Order 2 - record 3 created
        OrderDimension thirdOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        System.out.println(thirdOutputRecord.validTo.getTime());
        System.out.println(thirdOutputRecord);
        assertEquals(new Timestamp(record3.tsMs), thirdOutputRecord.validFrom);
        assertEquals(new Timestamp(END_OF_TIME), thirdOutputRecord.validTo);
        assertEquals(record3.status, thirdOutputRecord.status);
    }

}
