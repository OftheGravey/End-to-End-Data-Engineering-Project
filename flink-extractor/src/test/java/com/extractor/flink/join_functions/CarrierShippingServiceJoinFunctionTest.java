package com.extractor.flink.join_functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.*;

import com.extractor.flink.model.joined.CarrierService;
import com.extractor.flink.model.source.Carrier;
import com.extractor.flink.model.source.ShippingService;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CarrierShippingServiceJoinFunctionTest {

    private KeyedTwoInputStreamOperatorTestHarness<Integer, ShippingService, Carrier, CarrierService> testHarness;

    @BeforeEach
    public void setup() throws Exception {
        CarrierShippingServiceJoinFunction joinFunction = new CarrierShippingServiceJoinFunction();
        KeyedCoProcessOperator<Integer, ShippingService, Carrier, CarrierService> operator =
                new KeyedCoProcessOperator<>(joinFunction);

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                (ShippingService s) -> s.serviceId,
                (Carrier c) -> c.carrierId,
                TypeInformation.of(Integer.class)
        );

        testHarness.open();
    }

    @AfterEach
    public void cleanup() throws Exception {
        testHarness.close();
    }

    @Test
    public void testServiceArrivesFirst_thenCarrier() throws Exception {
        ShippingService service = new ShippingService();
        service.serviceId = 1;
        service.carrierId = 100;
        service.serviceName = "Express";

        Carrier carrier = new Carrier();
        carrier.carrierId = 100;
        carrier.name = "FastShip";

        testHarness.processElement1(service, 0);
        assertTrue(testHarness.extractOutputValues().isEmpty());

        testHarness.processElement2(carrier, 1);
        List<CarrierService> results = testHarness.extractOutputValues();
        assertEquals(1, results.size());

        CarrierService joined = results.get(0);
        assertEquals(1, joined.shippingService.serviceId);
        assertEquals("FastShip", joined.carrier.name);
    }

    @Test
    public void testCarrierArrivesFirst_thenService() throws Exception {
        Carrier carrier = new Carrier();
        carrier.carrierId = 200;
        carrier.name = "QuickShip";

        ShippingService service = new ShippingService();
        service.serviceId = 2;
        service.carrierId = 200;
        service.serviceName = "Standard";

        testHarness.processElement2(carrier, 0);
        assertTrue(testHarness.extractOutputValues().isEmpty());

        testHarness.processElement1(service, 1);
        List<CarrierService> results = testHarness.extractOutputValues();
        assertEquals(1, results.size());

        CarrierService joined = results.get(0);
        assertEquals("Standard", joined.shippingService.serviceName);
        assertEquals("QuickShip", joined.carrier.name);
    }

    @Test
    public void testMultipleServicesForSameCarrier() throws Exception {
        Carrier carrier = new Carrier();
        carrier.carrierId = 300;
        carrier.name = "GlobalShip";

        ShippingService service1 = new ShippingService();
        service1.serviceId = 3;
        service1.carrierId = 300;
        service1.serviceName = "Overnight";

        ShippingService service2 = new ShippingService();
        service2.serviceId = 4;
        service2.carrierId = 300;
        service2.serviceName = "Two-Day";

        testHarness.processElement1(service1, 0);
        testHarness.processElement1(service2, 1);
        assertTrue(testHarness.extractOutputValues().isEmpty());

        testHarness.processElement2(carrier, 2);
        List<CarrierService> results = testHarness.extractOutputValues();
        assertEquals(2, results.size());

        assertEquals("Overnight", results.get(0).shippingService.serviceName);
        assertEquals("Two-Day", results.get(1).shippingService.serviceName);
        assertEquals("GlobalShip", results.get(0).carrier.name);
    }
}
