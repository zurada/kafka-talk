package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.Dummy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDefaultConfigurationApplicationTests extends TestDriverStreamTest {

    @Test
    public void testExampleKafkaStreams() {
        Dummy dummyObject = new Dummy();
        pipeInput("dummy", "key", dummyObject);
        ProducerRecord<String, Dummy> result = readOutput("dummyOut");
        assertThat(result.value().getDummyUuid()).isEqualTo(dummyObject.getDummyUuid());
    }

}
