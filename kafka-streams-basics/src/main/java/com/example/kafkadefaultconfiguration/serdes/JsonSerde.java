package com.example.kafkadefaultconfiguration.serdes;

import com.example.kafkadefaultconfiguration.model.UserBalance;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class JsonSerde<T> implements Deserializer<T>, Serializer<T> {
    private final ObjectMapper objectMapper;
    private final ObjectReader reader;

    public JsonSerde(Class<T> targetType) {
        this.objectMapper = getObjectMapper();
        this.reader = objectMapper.readerFor(targetType);
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        } else {
            try {
                return this.reader.readValue(data);
            } catch (IOException e) {
                throw new SerializationException(
                        "Can't deserialize data [" + Arrays.toString(data) + "] from topic [" + topic + "]", e);
            }
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        return serialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        } else {
            try {
                return this.objectMapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new SerializationException("Can't serialize data [" + data + "] for topic [" + topic + "]", e);
            }
        }
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}

    public  ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }


    public static <T> Produced<String, T> produce(Class<T> cls){
        return Produced.with(Serdes.String(),  Serdes.serdeFrom(new JsonSerde(cls), new JsonSerde(cls)));
    }

    public static <T> Consumed<String, T> consume(Class<T> cls){
        return Consumed.with(Serdes.String(),  Serdes.serdeFrom(new JsonSerde(cls), new JsonSerde(cls)));
    }

    public static <T>Grouped<String, T> grouped(Class<T> cls, String repartitionTopicName){
        return Grouped.with(repartitionTopicName, Serdes.String(), Serdes.serdeFrom(new JsonSerde(cls), new JsonSerde(cls)));
    }

    public static <T> Materialized<String, T, KeyValueStore<Bytes, byte[]>> materialized(Class<T> cls) {
       return Materialized.with(Serdes.String(), Serdes.serdeFrom(new JsonSerde<>(cls), new JsonSerde<>(cls)));
    }
}
