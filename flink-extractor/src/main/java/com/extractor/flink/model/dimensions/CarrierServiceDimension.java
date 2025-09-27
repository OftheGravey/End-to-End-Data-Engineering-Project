package com.extractor.flink.model.dimensions;

import java.util.UUID;

import com.extractor.flink.model.joined.CarrierService;

public class CarrierServiceDimension extends TargetDimensionRecord {
    public String carrierServiceSk;
    public String serviceName;
    public String carrierName;
    public Integer serviceId;
    public String carrierContactEmail;
    public String carrierPhone;
    public Integer estimatedDays;
    public Double costEstimate;

    public CarrierServiceDimension(CarrierService record, Long validTo) {
        super(record, validTo);
        this.carrierServiceSk = UUID.randomUUID().toString();
        this.serviceName = record.shippingService.serviceName;
        this.carrierName = record.carrier.name;
        this.serviceId = record.shippingService.serviceId;
        this.carrierContactEmail = record.carrier.contactEmail;
        this.carrierPhone = record.carrier.phone;
        this.estimatedDays = record.shippingService.estimatedDays;
        this.costEstimate = record.shippingService.costEstimate;
    }

    public CarrierServiceDimension() {
    };

    @Override
    public CarrierServiceDimension clone(Long validTo) {
        CarrierServiceDimension newRecord = new CarrierServiceDimension();

        newRecord.carrierServiceSk = this.carrierServiceSk;
        newRecord.serviceName = this.serviceName;
        newRecord.serviceId = this.serviceId;
        newRecord.carrierName = this.carrierName;
        newRecord.carrierContactEmail = this.carrierContactEmail;
        newRecord.carrierPhone = this.carrierPhone;
        newRecord.estimatedDays = this.estimatedDays;
        newRecord.costEstimate = this.costEstimate;
        newRecord.validFrom = this.validFrom;
        newRecord.validTo = validTo;
        return newRecord;
    }
}