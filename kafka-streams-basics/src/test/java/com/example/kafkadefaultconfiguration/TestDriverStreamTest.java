package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.serdes.JsonSerde;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

public abstract class TestDriverStreamTest {
    private final static String KAFKA_STREAMS_PATH = "./tmp/test/kafka-streams/";

    @Autowired
    private StreamsBuilder builder;

    protected TopologyTestDriver driver;

    @Before
    public void setUp() {
        String stateDir = KAFKA_STREAMS_PATH + UUID.randomUUID().toString();
        Properties streamsConfigs = new Properties();
        streamsConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1111"); // dummy value
        streamsConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy"); //dummy value
        streamsConfigs.put(StreamsConfig.STATE_DIR_CONFIG, stateDir); //dummy value
        driver = new TopologyTestDriver(builder.build(), streamsConfigs);
    }

    private <T> T getStore(String name, QueryableStoreType<T> queryableStoreType) {
        StateStore store = driver.getStateStore(name);
        return queryableStoreType.accepts(store) ? (T)store : null;
    }

    @After
    public void cleanup () {
        try {
            driver.close();
        } catch(Exception e) {
            // silently ignore exceptions while closing (known issue on windows)
        }
    }

    @BeforeClass
    public static void prepare () {
        // cleanup old temp folder if not deleted on cleanup (known issue on windows)
        FileSystemUtils.deleteRecursively(new File(KAFKA_STREAMS_PATH));
    }

    protected  <V> void pipeInput(String topic, String key, V value) {
        @SuppressWarnings("unchecked") Class<V> valueClass = (Class<V>)value.getClass();
        ConsumerRecordFactory<String, V> factory = new ConsumerRecordFactory<>(Serdes.String().serializer(), new JsonSerde<>(valueClass));
        driver.pipeInput(factory.create(topic, key, value));
    }

    @SafeVarargs
    protected final <V> ProducerRecord<String, V> readOutput(String topic, V... dummy) {
        @SuppressWarnings("unchecked") Class<V> valueClass = (Class<V>)dummy.getClass().getComponentType();
        return driver.readOutput(topic, Serdes.String().deserializer(), new JsonSerde<>(valueClass));
    }
}
