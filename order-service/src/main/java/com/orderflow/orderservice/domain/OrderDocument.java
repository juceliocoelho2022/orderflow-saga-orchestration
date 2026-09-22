package com.orderflow.orderservice.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.orderflow.contracts.ProductItem;
import com.orderflow.contracts.SagaEvent;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

@Document("orders")
public class OrderDocument {

    @Id
    public String id;

    public String transactionId;
    public String correlationId;
    public List<ProductItem> products;
    public String status;
    public Instant createdAt;

    @JsonIgnore
    public SagaEvent pendingStartEvent;

    @JsonIgnore
    public Instant startEventPublishedAt;

    public OrderDocument() {
    }
}
