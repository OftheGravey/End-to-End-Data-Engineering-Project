package com.extractor.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Type;
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

import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;

public class OrdersDimensionJobTest extends OrdersDimensionJob {
    private KeyedOneInputStreamOperatorTestHarness<Integer, Order, OrderDimension> testHarness;
    private SCD2ProcessFunction<Order, OrderDimension> processFunction;
    private static final long END_OF_TIME = 253402300799000L;

    @BeforeEach
    public void setup() throws Exception {
        processFunction = new SCD2ProcessFunction<Order, OrderDimension>(TypeInformation.of(Order.class),
                TypeInformation.of(OrderDimension.class), OrderDimension::new);

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
        assertEquals(record1.tsMs, firstOutputRecord.validFrom);
        assertEquals(END_OF_TIME, firstOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 closed
        OrderDimension secondOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, secondOutputRecord.validFrom);
        assertEquals(record2.tsMs - 1, secondOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 2 updated
        OrderDimension fourthOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record2.tsMs, fourthOutputRecord.validFrom);
        assertEquals(END_OF_TIME, fourthOutputRecord.validTo);
        assertEquals(record2.status, fourthOutputRecord.status);

        // Order 2 - record 3 created
        OrderDimension thirdOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record3.tsMs, thirdOutputRecord.validFrom);
        assertEquals(END_OF_TIME, thirdOutputRecord.validTo);
        assertEquals(record3.status, thirdOutputRecord.status);
    }

    @Test
    public void testDeleteKeyedStream() throws Exception {
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
        record2.op = "d";
        record2.status = "cancelled";
        record2.shippingMethod = shippingMethod;
        record2.orderDate = orderDate;

        // Process the record
        testHarness.processElement(record1, record1.tsMs);
        testHarness.processElement(record2, record2.tsMs);

        // Advance time to trigger timer
        testHarness.setProcessingTime(10000L);

        // Verify output
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());

        // Order 1 - record 1 created
        OrderDimension firstOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, firstOutputRecord.validFrom);
        assertEquals(END_OF_TIME, firstOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 closed
        OrderDimension secondOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, secondOutputRecord.validFrom);
        assertEquals(record2.tsMs - 1, secondOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 deleted
        OrderDimension thirdOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record2.tsMs, thirdOutputRecord.validFrom);
        assertEquals(END_OF_TIME, thirdOutputRecord.validTo);
        assertEquals(record2.status, thirdOutputRecord.status);
    }

    @Test
    public void testMultiInsert() throws Exception {
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
        record2.status = "cancelled";
        record2.shippingMethod = shippingMethod;
        record2.orderDate = orderDate;
        // Record 3
        Order record3 = new Order();
        record3.orderId = 1;
        record3.tsMs = 3000L;
        record3.op = "u";
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
        assertEquals(5, output.size());

        // Order 1 - record 1 created
        OrderDimension firstOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, firstOutputRecord.validFrom);
        assertEquals(END_OF_TIME, firstOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 closed
        OrderDimension secondOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, secondOutputRecord.validFrom);
        assertEquals(record2.tsMs - 1, secondOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 deleted
        OrderDimension thirdOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record2.tsMs, thirdOutputRecord.validFrom);
        assertEquals(END_OF_TIME, thirdOutputRecord.validTo);
        assertEquals(record2.status, thirdOutputRecord.status);
    }

    @Test
    public void testDoubleCreateKeyedStream() throws Exception {
        // Standard un-important values
        String shippingMethod = "IGNORE";
        Long orderDate = 0L;

        // Record 1
        Order record1 = new Order();
        record1.orderId = 20705;
        record1.tsMs = 1753177299838L;
        record1.op = "c";
        record1.status = "pending";
        record1.shippingMethod = shippingMethod;
        record1.orderDate = orderDate;
        // Record 2
        Order record2 = new Order();
        record2.orderId = 20705;
        record2.tsMs = 1753506450424L;
        record2.op = "u";
        record2.status = "pending";
        record2.shippingMethod = shippingMethod;
        record2.orderDate = orderDate;

        // Process the record
        testHarness.processElement(record1, record1.tsMs);
        testHarness.processElement(record2, record2.tsMs);

        // Advance time to trigger timer
        testHarness.setProcessingTime(1753177299850L);

        // Verify output
        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());

        // Order 1 - record 1 created
        OrderDimension firstOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, firstOutputRecord.validFrom);
        assertEquals(END_OF_TIME, firstOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 closed
        OrderDimension secondOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record1.tsMs, secondOutputRecord.validFrom);
        assertEquals(record2.tsMs - 1, secondOutputRecord.validTo);
        assertEquals(record1.status, firstOutputRecord.status);

        // Order 1 - record 1 deleted
        OrderDimension thirdOutputRecord = (OrderDimension) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(record2.tsMs, thirdOutputRecord.validFrom);
        assertEquals(END_OF_TIME, thirdOutputRecord.validTo);
        assertEquals(record2.status, thirdOutputRecord.status);
    }

}
