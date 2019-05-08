package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.UserBalance;
import com.example.kafkadefaultconfiguration.serdes.JsonSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Properties;

@Configuration
public class KafkaConfig {


    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    @Lazy
    public ReadOnlyKeyValueStore<String, UserBalance> balanceStore(
            KafkaStreams kafkaStreams,
            GlobalKTable<String, UserBalance> globalBalanceTable) {
        return kafkaStreams.store(globalBalanceTable.queryableStoreName(), QueryableStoreTypes.keyValueStore());
    }

    @Bean
    public GlobalKTable<String, UserBalance> globalBalanceTable(StreamsBuilder builder) {
        return builder.globalTable("userBalances",
                JsonSerde.consume(UserBalance.class),
                Materialized.as("queryGlobalUserBalances"));
    }

    @Bean("streamsBuilder")
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean("kafkaStreams")
    public KafkaStreams kafkaStreams(
            StreamsBuilder builder) {
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "queriesConsumerGroup");
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "./tmp/kafka-streams");
        streamsConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        Topology topology = builder.build();
        logger.info("Streams topology:\n{}", topology.describe());

        return new KafkaStreams(topology, streamsConfig);
    }


    @Bean("kafkaStreamsLifecycle")
    public KafkaStreamsLifecycle kafkaStreamsLifecycle(KafkaStreams kafkaStreams) {
        return new KafkaStreamsLifecycle(kafkaStreams);
    }

    //SmaltLifecycle coming from Spring
    public static class KafkaStreamsLifecycle implements SmartLifecycle {

        private final EnumSet<KafkaStreams.State> RUNNING_STATES =
                EnumSet.of(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING);

        private final KafkaStreams kafkaStreams;

        public KafkaStreamsLifecycle(KafkaStreams kafkaStreams) {
            this.kafkaStreams = Objects.requireNonNull(kafkaStreams);
        }

        @Override
        public void stop() {
            stop(null);
        }

        @Override
        public void stop(Runnable callback) {
            if (callback != null) {
                callback.run();
            }

            if (isRunning()) {
                kafkaStreams.close(Duration.ofSeconds(10));
            }
        }

        @Override
        public void start() {
            if (!isRunning()) {
                try {
                    kafkaStreams.start();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start kafka streams", e);
                }
            }
        }

        @Override
        public boolean isRunning() {
            return RUNNING_STATES.contains(kafkaStreams.state());
        }

    }

}
