package com.extractor.flink.model.source;

public class Carrier extends DebeziumSourceRecord {
    public Integer carrierId;
    public String name;
    public String contactEmail;
    public String phone;

    // Default constructor
    public Carrier() {
    }

    @Override
    public String toString() {
        return String.format("Carrier{carrierId=%d, op='%s'}", carrierId, op);
    }
}