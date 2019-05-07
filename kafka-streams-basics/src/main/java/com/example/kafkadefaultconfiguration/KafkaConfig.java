package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.InvalidMoneyTransfer;
import com.example.kafkadefaultconfiguration.model.MoneyTransfer;
import com.example.kafkadefaultconfiguration.model.UserBalance;
import com.example.kafkadefaultconfiguration.serdes.JsonSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Properties;

@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    @SuppressWarnings("unchecked")
    public KStream<String, MoneyTransfer> moneyTransfers(StreamsBuilder streamsBuilder) {
        KStream<String, MoneyTransfer> moneyTransfers[] = streamsBuilder
                .stream("transfers", JsonSerde.consume(MoneyTransfer.class))
                .peek((key, moneyTransfer) -> logger.info("Received MoneyTransfer: {}", moneyTransfer))
                .filter((key, moneyTransfer) -> !moneyTransfer.getFromUser().equals(moneyTransfer.getToUser()))
                .branch((key, value) -> value.getAmount().doubleValue() > 0.0, (key, value) -> true);

        KStream<String, MoneyTransfer> validTransfers = moneyTransfers[0];
        KStream<String, MoneyTransfer> invalidTransfers = moneyTransfers[1];

        invalidTransfers.peek((key, moneyTransfer) -> logger.error("Invalid: amount below 0! for {}", moneyTransfer))
                .mapValues((readOnlyKey, value) -> new InvalidMoneyTransfer(value, "Invalid: amount below 0!"))
                .selectKey((key, value) ->  value.getMoneyTransfer().getFromUser()+ ":" + key)
                .to("invalidTransfers", JsonSerde.produce(InvalidMoneyTransfer.class));

        calculateUserBalances(validTransfers.groupBy((key, moneyTransfer) -> moneyTransfer.getFromUser(),
                JsonSerde.grouped(MoneyTransfer.class, "transferGroupedByFromUser")));
        calculateUserBalances(validTransfers.groupBy((key, moneyTransfer) -> moneyTransfer.getToUser(),
                JsonSerde.grouped(MoneyTransfer.class, "transferGroupedByToUser")));


        return validTransfers;
    }

    private void calculateUserBalances(KGroupedStream<String, MoneyTransfer> transfersGroupedByUser) {
        transfersGroupedByUser
                .aggregate(UserBalance::new, (key, moneyTransfer, userBalance) -> {
                    userBalance.setUserName(key);
                    BigDecimal newBalance;
                    if (userBalance.getUserName().equals(moneyTransfer.getFromUser())) {
                        newBalance = userBalance.getBalance().subtract(moneyTransfer.getAmount());
                    } else {
                        newBalance = userBalance.getBalance().add(moneyTransfer.getAmount());
                    }
                    userBalance.setBalance(newBalance);
                    return userBalance;
                }, JsonSerde.materialized(UserBalance.class))
                .toStream()
                .peek((key, userBalance) -> logger.info("Sending userBalance: {}", userBalance))
                .to("userBalances", JsonSerde.produce(UserBalance.class));
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
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "exampleConsumerGroup");
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
