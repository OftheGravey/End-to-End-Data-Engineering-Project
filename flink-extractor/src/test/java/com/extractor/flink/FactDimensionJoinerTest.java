package com.extractor.flink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.function.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;

import static org.junit.jupiter.api.Assertions.*;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.FactDimensionJoiner;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;

import lombok.Data;

public class FactDimensionJoinerTest {
    private static final long END_OF_TIME = 253402300799000L;
    private KeyedTwoInputStreamOperatorTestHarness<Integer, TestFact, TestDimension, TestEnrichedFact> testHarness;
    private TestFactDimensionJoiner processFunction;

    public static class TestFact extends DebeziumSourceRecord {
        public Integer factId;
        public Integer dimensionSourceId;
    }

    public static class TestDimension extends TargetDimensionRecord {
        public String dimensionSynthId;
        public Integer dimensionSourceId;
    }

    @Data
    public static class TestEnrichedFact {
        public Integer factId;
        public Integer dimensionSourceId;
        public String dimensionSynthId;
    }

    // Concrete implementation for testing
    private static class TestFactDimensionJoiner extends FactDimensionJoiner<TestFact, TestDimension, TestEnrichedFact> {
        public TestFactDimensionJoiner() {
            super(TypeInformation.of(TestFact.class), TypeInformation.of(TestDimension.class));
        }

        public TestEnrichedFact createJoinedDimension(TestFact fact, TestDimension dimension) {
            TestEnrichedFact enrichedFact = new TestEnrichedFact();
            enrichedFact.factId = fact.factId;
            enrichedFact.dimensionSourceId = fact.dimensionSourceId;
            enrichedFact.dimensionSynthId = dimension.dimensionSynthId;
            return enrichedFact;
        }
    }

    @BeforeEach
    public void setup() throws Exception {
        processFunction = new TestFactDimensionJoiner();
        
        // Create the keyed process operator with explicit types
        KeyedCoProcessOperator<Integer, TestFact, TestDimension, TestEnrichedFact> operator = 
            new KeyedCoProcessOperator<Integer, TestFact, TestDimension, TestEnrichedFact>(processFunction);
        
        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                record -> record.dimensionSourceId,
                record -> record.dimensionSourceId,
                TypeInformation.of(Integer.class)
        );
        testHarness.open();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    public void testSimpleJoinDimFirst() throws Exception {
        TestDimension dimension = new TestDimension();
        dimension.dimensionSourceId = 1;
        dimension.dimensionSynthId = "A";
        dimension.validFrom = new Timestamp(0L);
        dimension.validTo = new Timestamp(END_OF_TIME);
        TestFact fact = new TestFact();
        fact.dimensionSourceId = 1;
        fact.factId = 11;
        fact.tsMs = 1000L;
        // Expectation
        TestEnrichedFact eFact = new TestEnrichedFact();
        eFact.factId = 11;
        eFact.dimensionSynthId = "A";
        eFact.dimensionSourceId = 1;

        testHarness.processElement2(dimension, 0L);
        testHarness.processElement1(fact, 1000L);

        testHarness.setProcessingTime(2000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());
        TestEnrichedFact outputFact = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFact, eFact);

    }

    @Test
    public void testSimpleJoinFactFirst() throws Exception {
        TestDimension dimension = new TestDimension();
        dimension.dimensionSourceId = 1;
        dimension.dimensionSynthId = "A";
        dimension.validFrom = new Timestamp(0L);
        dimension.validTo = new Timestamp(END_OF_TIME);
        TestFact fact = new TestFact();
        fact.dimensionSourceId = 1;
        fact.factId = 11;
        fact.tsMs = 1000L;
        // Expectation
        TestEnrichedFact eFact = new TestEnrichedFact();
        eFact.factId = 11;
        eFact.dimensionSynthId = "A";
        eFact.dimensionSourceId = 1;

        testHarness.processElement1(fact, 0L);
        testHarness.processElement2(dimension, 1000L);

        testHarness.setProcessingTime(2000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());
        TestEnrichedFact outputFact = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFact, eFact);
    }

    @Test
    public void testOutOfDateDimension() throws Exception {
        TestDimension dimension = new TestDimension();
        dimension.dimensionSourceId = 1;
        dimension.dimensionSynthId = "A";
        dimension.validFrom = new Timestamp(0L);
        dimension.validTo = new Timestamp(10L); // Old dimension
        TestFact fact = new TestFact();
        fact.dimensionSourceId = 1;
        fact.factId = 11;
        fact.tsMs = 1000L;

        testHarness.processElement2(dimension, 0L);
        testHarness.processElement1(fact, 1000L);

        testHarness.setProcessingTime(2000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(0, output.size());
    }

    @Test
    public void testFutureDimension() throws Exception {
        TestDimension dimension = new TestDimension();
        dimension.dimensionSourceId = 1;
        dimension.dimensionSynthId = "A";
        dimension.validFrom = new Timestamp(1000L);  // Too New dimension
        dimension.validTo = new Timestamp(END_OF_TIME);
        TestFact fact = new TestFact();
        fact.dimensionSourceId = 1;
        fact.factId = 11;
        fact.tsMs = 0L;

        testHarness.processElement2(dimension, 1000L);
        testHarness.processElement1(fact, 2000L);

        testHarness.setProcessingTime(3000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(0, output.size());
    }

    @Test
    public void testMultipleVersionsDimensions() throws Exception {
        TestDimension dimensionA = new TestDimension();
        dimensionA.dimensionSourceId = 1;
        dimensionA.dimensionSynthId = "A";
        dimensionA.validFrom = new Timestamp(0L);  // Too New dimension
        dimensionA.validTo = new Timestamp(999L);
        TestDimension dimensionB = new TestDimension();
        dimensionB.dimensionSourceId = 1;
        dimensionB.dimensionSynthId = "B";
        dimensionB.validFrom = new Timestamp(1000L);  // Just right!
        dimensionB.validTo = new Timestamp(1999L);
        TestDimension dimensionC = new TestDimension();
        dimensionC.dimensionSourceId = 1;
        dimensionC.dimensionSynthId = "C";
        dimensionC.validFrom = new Timestamp(2000L);  // Too Old dimension
        dimensionC.validTo = new Timestamp(3000L);

        TestFact fact = new TestFact();
        fact.dimensionSourceId = 1;
        fact.factId = 11;
        fact.tsMs = 1000L;
        
        testHarness.processElement1(fact, 0L);
        testHarness.processElement2(dimensionA, 0L);
        testHarness.processElement2(dimensionB, 1000L);
        testHarness.processElement2(dimensionC, 2000L);
        // Timing for fact is irrelevant

        testHarness.setProcessingTime(4000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());

        TestEnrichedFact outputFact = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFact.dimensionSynthId, "B");
    }

    @Test
    public void testMultipleVersionsDimensionsLateFact() throws Exception {
        TestDimension dimensionA = new TestDimension();
        dimensionA.dimensionSourceId = 1;
        dimensionA.dimensionSynthId = "A";
        dimensionA.validFrom = new Timestamp(0L);  // Too New dimension
        dimensionA.validTo = new Timestamp(999L);
        TestDimension dimensionB = new TestDimension();
        dimensionB.dimensionSourceId = 1;
        dimensionB.dimensionSynthId = "B";
        dimensionB.validFrom = new Timestamp(1000L);  // Just right!
        dimensionB.validTo = new Timestamp(1999L);
        TestDimension dimensionC = new TestDimension();
        dimensionC.dimensionSourceId = 1;
        dimensionC.dimensionSynthId = "C";
        dimensionC.validFrom = new Timestamp(2000L);  // Too Old dimension
        dimensionC.validTo = new Timestamp(3000L);

        TestFact fact = new TestFact();
        fact.dimensionSourceId = 1;
        fact.factId = 11;
        fact.tsMs = 1000L;
        
        testHarness.processElement2(dimensionA, 0L);
        testHarness.processElement2(dimensionB, 1000L);
        testHarness.processElement2(dimensionC, 2000L);
        
        testHarness.processElement1(fact, 3000L); // Fact is Late!
        
        testHarness.setProcessingTime(4000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(1, output.size());

        TestEnrichedFact outputFact = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFact.dimensionSynthId, "B");
    }


    @Test
    public void testMultipleFactJoin() throws Exception {
        TestDimension dimension = new TestDimension();
        dimension.dimensionSourceId = 1;
        dimension.dimensionSynthId = "A";
        dimension.validFrom = new Timestamp(0L);
        dimension.validTo = new Timestamp(END_OF_TIME);

        TestFact factA = new TestFact();
        factA.dimensionSourceId = 1;
        factA.factId = 11;
        factA.tsMs = 1000L;

        TestFact factB = new TestFact();
        factB.dimensionSourceId = 1;
        factB.factId = 12;
        factB.tsMs = 1010L;

        TestFact factC = new TestFact();
        factC.dimensionSourceId = 1;
        factC.factId = 13;
        factC.tsMs = 1020L;

        testHarness.processElement2(dimension, 0L);
        testHarness.processElement1(factA, 10L);
        testHarness.processElement1(factB, 20L);
        testHarness.processElement1(factC, 30L);

        testHarness.setProcessingTime(2000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());
        TestEnrichedFact outputFactA = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFactA.factId, 11);
        System.out.println(outputFactA.factId);
        TestEnrichedFact outputFactB = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFactB.factId, 12);
        TestEnrichedFact outputFactC = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFactC.factId, 13);
    }

    @Test
    public void testMultipleFactJoinLateDimension() throws Exception {
        TestDimension dimension = new TestDimension();
        dimension.dimensionSourceId = 1;
        dimension.dimensionSynthId = "A";
        dimension.validFrom = new Timestamp(0L);
        dimension.validTo = new Timestamp(END_OF_TIME);

        TestFact factA = new TestFact();
        factA.dimensionSourceId = 1;
        factA.factId = 11;
        factA.tsMs = 1000L;

        TestFact factB = new TestFact();
        factB.dimensionSourceId = 1;
        factB.factId = 12;
        factB.tsMs = 1010L;

        TestFact factC = new TestFact();
        factC.dimensionSourceId = 1;
        factC.factId = 13;
        factC.tsMs = 1020L;

        testHarness.processElement1(factA, 0L);
        testHarness.processElement1(factB, 10L);
        testHarness.processElement1(factC, 20L);
        testHarness.processElement2(dimension, 1000L);

        testHarness.setProcessingTime(2000L);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
        assertEquals(3, output.size());
        TestEnrichedFact outputFactA = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFactA.factId, 11);
        System.out.println(outputFactA.factId);
        TestEnrichedFact outputFactB = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFactB.factId, 12);
        TestEnrichedFact outputFactC = (TestEnrichedFact) ((StreamRecord<?>) output.poll()).getValue();
        assertEquals(outputFactC.factId, 13);
    }
}