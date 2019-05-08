package com.example.kafkadefaultconfiguration.model;

import java.math.BigDecimal;

public class UserBalance {
    private String userName;
    private BigDecimal balance = BigDecimal.ZERO;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    @Override
    public String toString() {
        return "UserBalance{" +
                "userName='" + userName + '\'' +
                ", balance=" + balance +
                '}';
    }
}
