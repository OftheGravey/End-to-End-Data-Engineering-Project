package com.extractor.flink.model.dimensions;

import java.sql.Date;
import java.util.UUID;

import com.extractor.flink.model.source.Order;

public class OrderDimension extends TargetDimensionRecord {
		public Integer orderId;
		public String status;
		public String shippingMethod;
		public Date orderDate;
		public String orderSk;
		public Integer customerId;

		public OrderDimension(Order record, Long validTo) {
			super(record, validTo);
			this.status = record.status;
			this.customerId = record.customerId;
			this.orderId = record.orderId;
			this.shippingMethod = record.shippingMethod;
			this.orderDate = new Date(record.orderDate);
			this.orderSk = UUID.randomUUID().toString();
		}

		public OrderDimension() {
		};

		@Override
		public OrderDimension clone(Long validTo) {
			OrderDimension newRecord = new OrderDimension();
			newRecord.status = this.status;
			newRecord.orderId = this.orderId;
			newRecord.customerId = this.customerId;
			newRecord.shippingMethod = this.shippingMethod;
			newRecord.orderDate = this.orderDate;
			newRecord.orderSk = this.orderSk;
			newRecord.validFrom = this.validFrom;
			newRecord.validTo = validTo;
			return newRecord;
		}
	}

