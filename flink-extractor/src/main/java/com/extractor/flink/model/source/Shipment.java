package com.extractor.flink.model.source;

import java.sql.Date;

public class Shipment extends DebeziumSourceRecord {
    public Integer shipmentId;
    public Integer orderId;
    public Integer carrierId;
    public Integer serviceId;
    public String trackingNumber;
    public String shippingStatus;
    public Date shippedDate;
    public Date expectedDeliveryDate;
    public Date actualDeliveryDate;
    public Double shippingCost;
}