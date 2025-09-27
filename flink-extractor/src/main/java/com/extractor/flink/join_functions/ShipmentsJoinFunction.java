package com.extractor.flink.join_functions;

import org.apache.flink.util.Collector;

import java.util.Map;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

import com.extractor.flink.model.joined.ShipmentEventJoined;
import com.extractor.flink.model.source.Shipment;
import com.extractor.flink.model.source.ShipmentEvent;

public class ShipmentsJoinFunction
			extends KeyedCoProcessFunction<Integer, ShipmentEvent, Shipment, ShipmentEventJoined> {
		private transient MapState<Integer, ShipmentEvent> latestShipmentEventState;
		private transient ValueState<Shipment> latestShipmentState;

		@Override
		public void open(OpenContext ctx) throws Exception {
			latestShipmentEventState = getRuntimeContext().getMapState(new MapStateDescriptor<>("latest",
					TypeInformation.of(Integer.class), TypeInformation.of(ShipmentEvent.class)));
			latestShipmentState = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("latestCarrier", TypeInformation.of(Shipment.class)));
		}

		@Override
		public void processElement1(ShipmentEvent service, Context context, Collector<ShipmentEventJoined> out)
				throws Exception {
			latestShipmentEventState.put(service.serviceId, service);

			Shipment currentCarrier = latestShipmentState.value();
			if (currentCarrier != null) {
				out.collect(createJoinedDimension(service, currentCarrier));
			}
		}

		@Override
		public void processElement2(Shipment carrier, Context context, Collector<ShipmentEventJoined> out) throws Exception {
			latestShipmentState.update(carrier);

			Iterable<Map.Entry<Integer, ShipmentEvent>> services = latestShipmentEventState.entries();
			if (services != null) {
				for (Map.Entry<Integer, ShipmentEvent> entry : services) {
					ShipmentEvent currentBook = entry.getValue();
					out.collect(createJoinedDimension(currentBook, carrier));
				}
			}
		}

		private ShipmentEventJoined createJoinedDimension(ShipmentEvent shipmentEvent, Shipment shipment) {
			ShipmentEventJoined dim = new ShipmentEventJoined(shipment, shipmentEvent);
			return dim;
		}
	}