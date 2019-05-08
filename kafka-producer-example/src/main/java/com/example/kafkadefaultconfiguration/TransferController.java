package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.MoneyTransfer;
import io.swagger.annotations.Api;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.naming.ServiceUnavailableException;
import javax.validation.Valid;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("api")
@Api
public class TransferController {

    private static final Logger logger = LoggerFactory.getLogger(TransferController.class);
    private final Producer<String, MoneyTransfer> producer;

    public TransferController(Producer<String, MoneyTransfer> producer) {
        this.producer = producer;
    }

    @PostMapping("transfer")
    public ResponseEntity<Void> sendTransfer(
            @Valid @RequestBody MoneyTransfer moneyTransfer) {
        try {
            Future<RecordMetadata> send = producer.send(new ProducerRecord<>("transfers", UUID.randomUUID().toString(),moneyTransfer));
            send.get(5, TimeUnit.SECONDS);
            logger.info("Sent message {}", moneyTransfer);
        } catch(InterruptedException | ExecutionException | TimeoutException e) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }
}
