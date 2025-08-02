package com.extractor.flink.functions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class PojoDeserializer<T> implements DeserializationSchema<T> {

    private transient ObjectMapper objectMapper;
    private transient ObjectReader objectReader;
    private final Class<T> type;

    public PojoDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.objectReader = objectMapper.readerFor(type);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        //System.err.println("Failed to deserialize message: " + new String(message, StandardCharsets.UTF_8));
        try {
            return objectReader.readValue(new String(message, StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("Failed to deserialize message: " + new String(message, StandardCharsets.UTF_8));
            System.err.println("Error: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }
}