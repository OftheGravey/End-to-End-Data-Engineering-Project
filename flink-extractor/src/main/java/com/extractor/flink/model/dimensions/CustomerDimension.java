package com.extractor.flink.model.dimensions;

import java.util.UUID;

import com.extractor.flink.model.source.Customer;

public class CustomerDimension extends TargetDimensionRecord {
    public Integer customerId;
    public String email;
    public String phone;
    public String streetAddress;
    public String city;
    public String state;
    public String postalCode;
    public String country;
    public String customerSk;
    public String firstName;
    public String lastName;

    public CustomerDimension(Customer record, Long validTo) {
        super(record, validTo);
        this.customerId = record.customerId;
        this.email = record.email;
        this.phone = record.phone;
        this.streetAddress = record.streetAddress;
        this.city = record.city;
        this.state = record.state;
        this.postalCode = record.postalCode;
        this.country = record.country;
        this.firstName = record.firstName;
        this.lastName = record.lastName;
        this.customerSk = UUID.randomUUID().toString();
    }

    public CustomerDimension() {
    };

    @Override
    public CustomerDimension clone(Long validTo) {
        CustomerDimension newRecord = new CustomerDimension();
        newRecord.customerId = this.customerId;
        newRecord.email = this.email;
        newRecord.phone = this.phone;
        newRecord.streetAddress = this.streetAddress;
        newRecord.city = this.city;
        newRecord.state = this.state;
        newRecord.postalCode = this.postalCode;
        newRecord.country = this.country;
        newRecord.firstName = this.firstName;
        newRecord.lastName = this.lastName;
        newRecord.customerSk = this.customerSk;
        newRecord.validFrom = this.validFrom;
        newRecord.validTo = validTo;
        return newRecord;
    }
}