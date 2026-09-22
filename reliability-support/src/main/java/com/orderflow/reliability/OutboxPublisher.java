package com.orderflow.reliability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.contracts.SagaEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.concurrent.TimeUnit;

public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxMessageRepository repository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final long publishTimeoutMs;

    public OutboxPublisher(
            OutboxMessageRepository repository,
            ObjectMapper objectMapper,
            KafkaTemplate<String, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${orderflow.reliability.publish-timeout-ms:10000}") long publishTimeoutMs
    ) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @Scheduled(fixedDelayString = "${orderflow.reliability.outbox-delay-ms:1000}")
    public void publishPending() {
        for (var message : repository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) {
            try {
                var event = objectMapper.readValue(message.getPayload(), SagaEvent.class);
                kafkaTemplate.send(message.getTopic(), message.getMessageKey(), event)
                        .get(publishTimeoutMs, TimeUnit.MILLISECONDS);

                message.markPublished();
                repository.save(message);

                meterRegistry.counter(
                        "orderflow.outbox.published",
                        "topic", message.getTopic()
                ).increment();

                log.info(
                        "Outbox published topic={} eventId={} correlationId={}",
                        message.getTopic(),
                        event.eventId(),
                        event.correlationId()
                );
            } catch (Exception ex) {
                meterRegistry.counter(
                        "orderflow.outbox.publish.failures",
                        "topic", message.getTopic()
                ).increment();

                log.warn(
                        "Outbox publish failed topic={} eventId={}: {}",
                        message.getTopic(),
                        message.getEventId(),
                        ex.getMessage()
                );
            }
        }
    }
}
