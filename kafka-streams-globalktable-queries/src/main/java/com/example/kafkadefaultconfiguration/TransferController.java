package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.UserBalance;
import io.swagger.annotations.Api;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.inject.Provider;
import java.util.LinkedList;
import java.util.List;

@RestController
@RequestMapping("api")
@Api
public class TransferController {

    private final Provider<ReadOnlyKeyValueStore<String, UserBalance>> userBalancesProvider;

    public TransferController(Provider<ReadOnlyKeyValueStore<String, UserBalance>> userBalancesProvider) {
        this.userBalancesProvider = userBalancesProvider;
    }

    @GetMapping("transfer/all")
    public ResponseEntity<List<UserBalance>> sendTransfer() {
        List<UserBalance> allBalances = new LinkedList<>();
        KeyValueIterator<String, UserBalance> keyValueIterator = userBalancesProvider.get().all();
        while (keyValueIterator.hasNext()) {
            allBalances.add(keyValueIterator.next().value);
        }
        return ResponseEntity.status(HttpStatus.OK).body(allBalances);
    }
}
