package com.orderflow.orderservice.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Document("saga_history")
public class SagaHistory {

    @Id
    public String id;

    @Version
    public Long version;

    public String orderId;
    public String transactionId;
    public String correlationId;
    public String status;
    public List<String> events = new ArrayList<>();
    public List<String> processedEventIds = new ArrayList<>();
    public Instant updatedAt;

    public SagaHistory() {
    }
}
