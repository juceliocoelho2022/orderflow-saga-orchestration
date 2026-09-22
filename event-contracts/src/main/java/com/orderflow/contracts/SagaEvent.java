package com.orderflow.contracts;

import java.time.Instant;

public record SagaEvent(
        String eventId,
        String transactionId,
        String orderId,
        SagaStep source,
        SagaStatus status,
        String message,
        OrderPayload payload,
        String correlationId,
        Instant createdAt
) {
    public SagaEvent {
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = transactionId;
        }
    }

    public SagaEvent(
            String eventId,
            String transactionId,
            String orderId,
            SagaStep source,
            SagaStatus status,
            String message,
            OrderPayload payload,
            Instant createdAt
    ) {
        this(eventId, transactionId, orderId, source, status, message, payload, transactionId, createdAt);
    }
}
