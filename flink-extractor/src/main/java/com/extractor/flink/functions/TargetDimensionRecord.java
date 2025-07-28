package com.extractor.flink.functions;

import java.io.Serializable;
import java.sql.Timestamp;

import lombok.Data;

@Data
public class TargetDimensionRecord implements Serializable {
    public Timestamp validTo;
    public Timestamp validFrom; 
    public TargetDimensionRecord(DebeziumSourceRecord record, Long validTo) {
        this.validFrom = new Timestamp(record.tsMs);
        this.validTo = new Timestamp(validTo);
    };

    public TargetDimensionRecord() {}

    public TargetDimensionRecord clone(Timestamp validTo) {
        TargetDimensionRecord newRecord = new TargetDimensionRecord();
        newRecord.validFrom = this.validFrom;
        newRecord.validTo = validTo;
        return newRecord;
    }

    public String toString(){
        return String.format("TargetRecord(validFrom=%s, validTo=%s)", validFrom, validTo);
    }

}