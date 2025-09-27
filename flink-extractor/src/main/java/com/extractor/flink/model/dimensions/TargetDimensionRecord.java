package com.extractor.flink.model.dimensions;

import java.io.Serializable;

import com.extractor.flink.model.source.DebeziumSourceRecord;

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