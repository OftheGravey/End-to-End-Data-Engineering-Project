package com.extractor.flink.model.joined;

import com.extractor.flink.model.dimensions.CarrierServiceDimension;
import com.extractor.flink.model.dimensions.OrderDimension;
import com.extractor.flink.model.source.DebeziumSourceRecord;
import com.extractor.flink.model.source.Shipment;
import com.extractor.flink.model.source.ShipmentEvent;

public class ShipmentEventJoined extends DebeziumSourceRecord {
    public Shipment shipment;
	public ShipmentEvent shipmentEvent;

    public OrderDimension order;
    public CarrierServiceDimension carrierService;

	public ShipmentEventJoined(Shipment shipment, ShipmentEvent shipmentEvent) {
		this.shipmentEvent = shipmentEvent;
		this.shipment = shipment;
		this.tsMs = shipmentEvent.tsMs;
		this.op = shipmentEvent.op;
	}
}
