package com.extractor.flink.functions;

import java.io.Serializable;

import lombok.Data;

@Data
public class DebeziumSourceRecord implements Serializable {
    public Long tsMs;
    public String op;
}