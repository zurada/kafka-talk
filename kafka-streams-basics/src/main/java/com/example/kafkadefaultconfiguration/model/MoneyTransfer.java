package com.example.kafkadefaultconfiguration.model;

import java.math.BigDecimal;

public class MoneyTransfer {
    private String fromUser;
    private String toUser;
    private BigDecimal amount;

    public String getFromUser() {
        return fromUser;
    }

    public void setFromUser(String fromUser) {
        this.fromUser = fromUser;
    }

    public String getToUser() {
        return toUser;
    }

    public void setToUser(String toUser) {
        this.toUser = toUser;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "MoneyTransfer{" +
                "fromUser='" + fromUser + '\'' +
                ", toUser='" + toUser + '\'' +
                ", amount=" + amount +
                '}';
    }
}
