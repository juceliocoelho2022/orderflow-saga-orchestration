package com.orderflow.reliability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.contracts.SagaEvent;

public class OutboxService {

    private final OutboxMessageRepository repository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxMessageRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public void enqueue(String topic, String key, SagaEvent event) {
        try {
            repository.save(new OutboxMessage(
                    event.eventId(),
                    topic,
                    key,
                    objectMapper.writeValueAsString(event)
            ));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Could not serialize SagaEvent for Outbox", ex);
        }
    }
}
