package com.extractor.flink.functions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

public class PojoSerializer<T> implements SerializationSchema<T> {
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return objectMapper.writeValueAsString(element).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.println("Failed to serialize OrderDimension to JSON: " + e.getMessage());
            return null;
        }
    }
}