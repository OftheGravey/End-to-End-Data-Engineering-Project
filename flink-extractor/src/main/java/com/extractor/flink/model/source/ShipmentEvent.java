package com.extractor.flink.model.source;

import java.sql.Date;

public class ShipmentEvent extends DebeziumSourceRecord {
		public Integer shipmentId;
		public Integer orderId;
		public Integer carrierId;
		public Integer serviceId;
		public Integer shipmentEventId;
		public String status;
		public String location;
		public String trackingNumber;
		public String shippingStatus;
		public Date shippedDate;
		public Date expectedDeliveryDate;
		public Date actualDeliveryDate;
		public Double shippingCost;
	}