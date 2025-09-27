package com.extractor.flink.join_functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.*;

import com.extractor.flink.model.joined.ShipmentEventJoined;
import com.extractor.flink.model.source.Shipment;
import com.extractor.flink.model.source.ShipmentEvent;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ShipmentsJoinFunctionTest {

    private KeyedTwoInputStreamOperatorTestHarness<Integer, ShipmentEvent, Shipment, ShipmentEventJoined> testHarness;

    @BeforeEach
    public void setup() throws Exception {
        ShipmentsJoinFunction joinFunction = new ShipmentsJoinFunction();
        KeyedCoProcessOperator<Integer, ShipmentEvent, Shipment, ShipmentEventJoined> operator =
                new KeyedCoProcessOperator<>(joinFunction);

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                (ShipmentEvent s) -> s.shipmentEventId,
                (Shipment s) -> s.shipmentId,
                TypeInformation.of(Integer.class)
        );

        testHarness.open();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    public void testShipmentEventArrivesFirst_thenShipment() throws Exception {
        ShipmentEvent shipmentEvent = new ShipmentEvent();
        shipmentEvent.shipmentEventId = 1;
        shipmentEvent.shipmentId = 100;
        shipmentEvent.status = "Created";

        Shipment shipment = new Shipment();
        shipment.shipmentId = 100;
        shipment.carrierId = 10;
        shipment.trackingNumber = "TRK123";

        testHarness.processElement1(shipmentEvent, 0);
        assertTrue(testHarness.extractOutputValues().isEmpty());

        testHarness.processElement2(shipment, 1);
        List<ShipmentEventJoined> results = testHarness.extractOutputValues();
        assertEquals(1, results.size());

        ShipmentEventJoined joined = results.get(0);
        assertEquals("Created", joined.shipmentEvent.status);
        assertEquals("TRK123", joined.shipment.trackingNumber);
    }

    @Test
    public void testShipmentArrivesFirst_thenShipmentEvent() throws Exception {
        Shipment shipment = new Shipment();
        shipment.shipmentId = 200;
        shipment.carrierId = 20;
        shipment.trackingNumber = "TRK456";

        ShipmentEvent shipmentEvent = new ShipmentEvent();
        shipmentEvent.shipmentEventId = 2;
        shipmentEvent.shipmentId = 200;
        shipmentEvent.status = "Shipped";

        testHarness.processElement2(shipment, 0);
        assertTrue(testHarness.extractOutputValues().isEmpty());

        testHarness.processElement1(shipmentEvent, 1);
        List<ShipmentEventJoined> results = testHarness.extractOutputValues();
        assertEquals(1, results.size());

        ShipmentEventJoined joined = results.get(0);
        assertEquals("Shipped", joined.shipmentEvent.status);
        assertEquals("TRK456", joined.shipment.trackingNumber);
    }

    @Test
    public void testMultipleShipmentEventsForSameShipment() throws Exception {
        Shipment shipment = new Shipment();
        shipment.shipmentId = 300;
        shipment.carrierId = 30;
        shipment.trackingNumber = "TRK789";

        ShipmentEvent event1 = new ShipmentEvent();
        event1.shipmentEventId = 3;
        event1.shipmentId = 300;
        event1.status = "PickedUp";

        ShipmentEvent event2 = new ShipmentEvent();
        event2.shipmentEventId = 4;
        event2.shipmentId = 300;
        event2.status = "InTransit";

        testHarness.processElement1(event1, 0);
        testHarness.processElement1(event2, 1);
        assertTrue(testHarness.extractOutputValues().isEmpty());

        testHarness.processElement2(shipment, 2);
        List<ShipmentEventJoined> results = testHarness.extractOutputValues();
        assertEquals(2, results.size());

        assertEquals("PickedUp", results.get(0).shipmentEvent.status);
        assertEquals("InTransit", results.get(1).shipmentEvent.status);
        assertEquals("TRK789", results.get(0).shipment.trackingNumber);
    }
}
