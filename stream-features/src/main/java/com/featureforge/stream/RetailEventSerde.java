package com.featureforge.stream;

import com.featureforge.proto.RetailEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public final class RetailEventSerde implements Serde<RetailEvent> {
    @Override
    public Serializer<RetailEvent> serializer() {
        return (topic, data) -> data == null ? null : data.toByteArray();
    }

    @Override
    public Deserializer<RetailEvent> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return RetailEvent.parseFrom(data);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse RetailEvent protobuf", e);
            }
        };
    }
}