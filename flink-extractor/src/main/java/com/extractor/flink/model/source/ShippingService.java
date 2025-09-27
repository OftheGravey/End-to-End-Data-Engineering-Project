package com.extractor.flink.model.source;

public class ShippingService extends DebeziumSourceRecord {
    public Integer carrierId;
    public Integer serviceId;
    public String serviceName;
    public Integer estimatedDays;
    public Double costEstimate;

    // Default constructor
    public ShippingService() {
    }

    @Override
    public String toString() {
        return String.format("ShippingService{carrierId=%d, op='%s'}", carrierId, op);
    }
}