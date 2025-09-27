package com.extractor.flink.join_functions;

import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.extractor.flink.model.joined.CarrierService;
import com.extractor.flink.model.source.Carrier;
import com.extractor.flink.model.source.ShippingService;

public class CarrierShippingServiceJoinFunction
        extends KeyedCoProcessFunction<Integer, ShippingService, Carrier, CarrierService> {
    private transient MapState<Integer, ShippingService> latestServiceState;
    private transient ValueState<Carrier> latestCarrierState;

    @Override
    public void open(OpenContext ctx) throws Exception {
        latestServiceState = getRuntimeContext().getMapState(new MapStateDescriptor<>("latestService",
                TypeInformation.of(Integer.class), TypeInformation.of(ShippingService.class)));
        latestCarrierState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("latestCarrier", TypeInformation.of(Carrier.class)));
    }

    @Override
    public void processElement1(ShippingService service, Context context, Collector<CarrierService> out)
            throws Exception {
        latestServiceState.put(service.serviceId, service);

        Carrier currentCarrier = latestCarrierState.value();
        if (currentCarrier != null) {
            out.collect(createJoinedDimension(service, currentCarrier));
        }
    }

    @Override
    public void processElement2(Carrier carrier, Context context, Collector<CarrierService> out) throws Exception {
        latestCarrierState.update(carrier);

        Iterable<Map.Entry<Integer, ShippingService>> services = latestServiceState.entries();
        if (services != null) {
            for (Map.Entry<Integer, ShippingService> entry : services) {
                ShippingService currentService = entry.getValue();
                out.collect(createJoinedDimension(currentService, carrier));
            }
        }
    }

    private CarrierService createJoinedDimension(ShippingService service, Carrier carrier) {
        CarrierService dim = new CarrierService(service, carrier);
        return dim;
    }
}