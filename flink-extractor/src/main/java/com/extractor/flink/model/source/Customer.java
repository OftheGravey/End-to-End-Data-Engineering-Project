package com.extractor.flink.model.source;

import java.security.Timestamp;

public class Customer extends DebeziumSourceRecord {
    public Integer customerId;
    public String firstName;
    public String lastName;
    public String email;
    public String phone;
    public Timestamp createdAt;
    public String streetAddress;
    public String city;
    public String state;
    public String postalCode;
    public String country;
    public Long emittedTsMs;
    public String connectorVersion;
    public String transactionId;
    public Long lsn;

    @Override
    public String toString() {
        return String.format("Customer{customerId=%d, city='%s', op='%s'}", customerId, city, op);
    }
}