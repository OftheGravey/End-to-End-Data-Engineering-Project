package com.extractor.flink.model.fact;

import java.sql.Date;
import java.util.UUID;

import org.apache.flink.api.common.functions.MapFunction;

import com.extractor.flink.model.joined.ShipmentEventJoined;

import lombok.Data;

@Data
public class ShipmentEventFact {
	public String shipmentEventSk;
	public String orderSk;
	public String carrierServiceSk;
	public Integer shipmentId;
	public Integer shipmentEventId;
	public String status;
	public String location;
	public String trackingNumber;
	public String shippingStatus;
	public Date shippedDate;
	public Date expectedDeliveryDate;
	public Date actualDeliveryDate;
	public Double shippingCost;

	public static class ShipmentEventFactMapping implements MapFunction<ShipmentEventJoined, ShipmentEventFact> {
		@Override
		public ShipmentEventFact map(ShipmentEventJoined shipmentEvent) {
			ShipmentEventFact shipmentEventFact = new ShipmentEventFact();
			shipmentEventFact.shipmentEventSk = UUID.randomUUID().toString();
			shipmentEventFact.orderSk = shipmentEvent.order.orderSk;
			shipmentEventFact.carrierServiceSk = shipmentEvent.carrierService.carrierServiceSk;
			shipmentEventFact.shipmentId = shipmentEvent.shipment.shipmentId;
			shipmentEventFact.shipmentEventId = shipmentEvent.shipmentEvent.shipmentEventId;
			shipmentEventFact.status = shipmentEvent.shipmentEvent.status;
			shipmentEventFact.location = shipmentEvent.shipmentEvent.location;
			shipmentEventFact.trackingNumber = shipmentEvent.shipment.trackingNumber;
			shipmentEventFact.shippingStatus = shipmentEvent.shipment.shippingStatus;
			shipmentEventFact.shippedDate = shipmentEvent.shipment.shippedDate;
			shipmentEventFact.expectedDeliveryDate = shipmentEvent.shipment.expectedDeliveryDate;
			shipmentEventFact.actualDeliveryDate = shipmentEvent.shipment.actualDeliveryDate;
			shipmentEventFact.shippingCost = shipmentEvent.shipment.shippingCost;

			return shipmentEventFact;
		}
	}
}