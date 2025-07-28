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
        // Initialize ObjectMapper in the open method
        this.objectMapper = new ObjectMapper();
        this.objectReader = objectMapper.readerFor(type); // Use the provided type
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        // Convert bytes to JSON string and then to POJO object
        return objectReader.readValue(new String(message, StandardCharsets.UTF_8));
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        // For continuous streams, this should always return false.
        // It's used for bounded streams to signal the end.
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        // Return the TypeInformation for the POJO
        return TypeInformation.of(type);
    }
}