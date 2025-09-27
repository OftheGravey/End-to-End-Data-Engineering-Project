package com.extractor.flink.model.joined;

import com.extractor.flink.model.source.Carrier;
import com.extractor.flink.model.source.DebeziumSourceRecord;
import com.extractor.flink.model.source.ShippingService;

public class CarrierService extends DebeziumSourceRecord {
	public Carrier carrier;
	public ShippingService shippingService;

	public CarrierService(ShippingService shippingService, Carrier carrier) {
		this.carrier = carrier;
		this.shippingService = shippingService;
		this.tsMs = shippingService.tsMs;
		this.op = shippingService.op;
	}
}