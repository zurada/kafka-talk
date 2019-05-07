package com.example.kafkadefaultconfiguration.model;

public class InvalidMoneyTransfer {
    private MoneyTransfer moneyTransfer;
    private String reason;

    public InvalidMoneyTransfer(MoneyTransfer moneyTransfer, String reason) {
        this.moneyTransfer = moneyTransfer;
        this.reason = reason;
    }
    //for serialization
    public InvalidMoneyTransfer() {
    }

    public MoneyTransfer getMoneyTransfer() {
        return moneyTransfer;
    }

    public void setMoneyTransfer(MoneyTransfer moneyTransfer) {
        this.moneyTransfer = moneyTransfer;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "InvalidMoneyTransfer{" +
                "moneyTransfer=" + moneyTransfer +
                ", reason='" + reason + '\'' +
                '}';
    }
}
