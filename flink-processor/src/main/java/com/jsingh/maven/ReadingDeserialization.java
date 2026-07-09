package com.jsingh.maven;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ReadingDeserialization implements DeserializationSchema<Reading> {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Reading deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Reading.class);
    }

    @Override
    public boolean isEndOfStream(Reading nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Reading> getProducedType() {
        return TypeInformation.of(Reading.class);
    }

}
