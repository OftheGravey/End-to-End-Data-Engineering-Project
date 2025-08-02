package com.extractor.flink.functions;

import java.io.Serializable;

import lombok.Data;

@Data
public class TargetDimensionRecord implements Serializable {
    public Long validTo;
    public Long validFrom; 
    public TargetDimensionRecord(DebeziumSourceRecord record, Long validTo) {
        this.validFrom = record.tsMs;
        this.validTo = validTo;
    };

    public TargetDimensionRecord() {}

    public TargetDimensionRecord clone(Long validTo) {
        TargetDimensionRecord newRecord = new TargetDimensionRecord();
        newRecord.validFrom = this.validFrom;
        newRecord.validTo = validTo;
        return newRecord;
    }

    public String toString(){
        return String.format("TargetRecord(validFrom=%s, validTo=%s)", validFrom, validTo);
    }

}