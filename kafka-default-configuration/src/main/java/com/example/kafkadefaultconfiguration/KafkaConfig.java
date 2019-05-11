package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.Dummy;
import com.example.kafkadefaultconfiguration.serdes.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Properties;

import static java.lang.String.format;

@Configuration
public class KafkaConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Autowired
    private ApplicationContext applicationContext;


    @Bean
    public KStream<String, Dummy> dummyStream(StreamsBuilder streamsBuilder) {
        KStream<String, Dummy> dummyStream = streamsBuilder
                .stream("dummy", JsonSerde.consume(Dummy.class))
                .peek((s, dummy) -> logger.info("Received Dummy: {}", dummy));
        dummyStream.to("dummyOut", JsonSerde.produce(Dummy.class));

        return dummyStream;
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
    public class KafkaStreamsLifecycle implements SmartLifecycle {

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
                    kafkaStreams.setStateListener(getStateListener());
                    kafkaStreams.setUncaughtExceptionHandler(getUncaughtExceptionHandler());
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

    /**
     * @return Default UncaughtExceptionHandler, where we try to close the Application on UncaughtException
     */
    private  Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return (t, e) -> {
            logger.info("Uncaught exception in KafkaStream: [{}]. Shutting down Spring application.", e);
            try {
            	System.exit(SpringApplication.exit(applicationContext, () -> 0));
			} catch(IllegalStateException ex) {
                // ignore - VM is already shutting down
            }
        };
    }

    /**
     * @return Default KafkaStreams.StateListener, where we try to close the Application on newState == NOT_RUNNING as
     * a last resort
     */
    private KafkaStreams.StateListener getStateListener() {
        return (newState, oldState) -> {
            logger.info("Detected KafkaStream state change: [{}] -> [{}]", oldState.name(), newState.name());
            if(KafkaStreams.State.NOT_RUNNING == newState) {
                logger.info("KafkaStream is moving into an unwanted state: [{}]. Shutting down Spring application.", newState);
                try {
                    System.exit(SpringApplication.exit(applicationContext, () -> 0));
                } catch(IllegalStateException ex) {
                    // ignore - VM is already shutting down
                }
            }
        };
    }

}
