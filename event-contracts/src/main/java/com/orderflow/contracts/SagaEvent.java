package com.orderflow.contracts;
import java.time.Instant;
public record SagaEvent(String eventId, String transactionId, String orderId, SagaStep source, SagaStatus status, String message, OrderPayload payload, Instant createdAt) {}
