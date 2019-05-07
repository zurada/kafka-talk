package com.example.kafkadefaultconfiguration;

import com.example.kafkadefaultconfiguration.model.InvalidMoneyTransfer;
import com.example.kafkadefaultconfiguration.model.MoneyTransfer;
import com.example.kafkadefaultconfiguration.model.UserBalance;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaDefaultConfigurationApplicationTests extends TestDriverStreamTest {

    @Test
    public void shouldIgnoreTransferBelowZero() {
        //given
        MoneyTransfer transfer = new MoneyTransfer();
        transfer.setAmount(BigDecimal.valueOf(-10));
        transfer.setFromUser("user1");
        transfer.setToUser("user2");

        //when
        pipeInput("transfers", "transfer1", transfer);

        //then
        ProducerRecord<String, UserBalance> result = readOutput("userBalances");
        assertThat(result).isNull();
        ProducerRecord<String, InvalidMoneyTransfer> invalidTransfer = readOutput("invalidTransfers");
        assertThat(invalidTransfer.value().getMoneyTransfer().getAmount()).isEqualTo(transfer.getAmount());
        assertThat(invalidTransfer.key()).isEqualTo(transfer.getFromUser()+":"+"transfer1");
    }

    @Test
    public void shouldTransferAndUpdateBalance() {
        //given
        MoneyTransfer transfer = new MoneyTransfer();
        transfer.setAmount(BigDecimal.TEN);
        transfer.setFromUser("user1");
        transfer.setToUser("user2");

        //when
        pipeInput("transfers", "transfer1", transfer);

        //then
        ProducerRecord<String, UserBalance> result = readOutput("userBalances");
        assertThatBalanceIs(result, "user1", -10);
        assertThatBalanceIs(result, "user2", 10);

        result = readOutput("userBalances");
        assertThatBalanceIs(result, "user1", -10);
        assertThatBalanceIs(result, "user2", 10);

        //when
        pipeInput("transfers", "transfer2", transfer);

        //then
        result = readOutput("userBalances");
        assertThatBalanceIs(result, "user1", -20);
        assertThatBalanceIs(result, "user2", 20);
        result = readOutput("userBalances");
        assertThatBalanceIs(result, "user1", -20);
        assertThatBalanceIs(result, "user2", 20);
    }

    private void assertThatBalanceIs(ProducerRecord<String, UserBalance> result, String user2, int i) {
        if (result.value().getUserName().equals(user2)) {
            assertThat(result.value().getBalance()).isEqualTo(BigDecimal.valueOf(i));
        }
    }

}
