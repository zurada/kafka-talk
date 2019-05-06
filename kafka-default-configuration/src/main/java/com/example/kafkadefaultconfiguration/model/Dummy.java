package com.example.kafkadefaultconfiguration.model;

import java.time.Instant;
import java.util.UUID;

public class Dummy {
    private Instant dummyDate;
    private UUID dummyUuid;

    public Dummy() {
        this.dummyDate = Instant.now();
        this.dummyUuid = UUID.randomUUID();
    }

    public Instant getDummyDate() {
        return dummyDate;
    }

    public void setDummyDate(Instant dummyDate) {
        this.dummyDate = dummyDate;
    }

    public UUID getDummyUuid() {
        return dummyUuid;
    }

    public void setDummyUuid(UUID dummyUuid) {
        this.dummyUuid = dummyUuid;
    }

    @Override
    public String toString() {
        return "Dummy{" +
                "dummyDate=" + dummyDate +
                ", dummyUuid=" + dummyUuid +
                '}';
    }
}
