package com.orderflow.orchestrator;

import com.orderflow.contracts.SagaEvent;
import com.orderflow.contracts.SagaStatus;
import com.orderflow.contracts.SagaStep;
import com.orderflow.contracts.Topics;
import com.orderflow.reliability.OutboxService;
import com.orderflow.reliability.ProcessedEvent;
import com.orderflow.reliability.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Component
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final ProcessedEventRepository processedEvents;
    private final OutboxService outbox;
    private final SagaMetrics metrics;

    public SagaOrchestrator(
            ProcessedEventRepository processedEvents,
            OutboxService outbox,
            SagaMetrics metrics
    ) {
        this.processedEvents = processedEvents;
        this.outbox = outbox;
        this.metrics = metrics;
    }

    @Transactional
    @KafkaListener(topics = Topics.START, groupId = "orchestrator-start")
    public void start(SagaEvent event) {
        if (alreadyProcessed(event)) {
            return;
        }

        enqueue(
                Topics.PRODUCT,
                event,
                SagaStep.ORCHESTRATOR,
                SagaStatus.STARTED,
                "Validate products"
        );
        markProcessed(event);

        log.info(
                "Saga started orderId={} transactionId={} correlationId={}",
                event.orderId(),
                event.transactionId(),
                event.correlationId()
        );
    }

    @Transactional
    @KafkaListener(topics = Topics.ORCHESTRATOR, groupId = "orchestrator-results")
    public void result(SagaEvent event) {
        if (alreadyProcessed(event)) {
            return;
        }

        if (event.source() == SagaStep.PRODUCT_VALIDATION) {
            handleProductResult(event);
        } else if (event.source() == SagaStep.PAYMENT) {
            handlePaymentResult(event);
        } else if (event.source() == SagaStep.INVENTORY) {
            handleInventoryResult(event);
        }

        markProcessed(event);
    }

    private void handleProductResult(SagaEvent event) {
        if (event.status() == SagaStatus.SUCCESS) {
            enqueue(
                    Topics.PAYMENT,
                    event,
                    SagaStep.ORCHESTRATOR,
                    SagaStatus.STARTED,
                    "Process payment"
            );
        } else {
            metrics.failed();
            finish(event, "Product validation failed");
        }
    }

    private void handlePaymentResult(SagaEvent event) {
        if (event.status() == SagaStatus.ROLLBACK) {
            metrics.compensated();
            finish(event, "Inventory failed; payment refunded");
            return;
        }

        if (event.status() == SagaStatus.SUCCESS) {
            enqueue(
                    Topics.INVENTORY,
                    event,
                    SagaStep.ORCHESTRATOR,
                    SagaStatus.STARTED,
                    "Reserve inventory"
            );
        } else {
            metrics.failed();
            finish(event, "Payment failed");
        }
    }

    private void handleInventoryResult(SagaEvent event) {
        if (event.status() == SagaStatus.SUCCESS) {
            metrics.completed();
            outbox.enqueue(
                    Topics.ORDER_NOTIFY,
                    event.orderId(),
                    next(
                            event,
                            SagaStep.ORCHESTRATOR,
                            SagaStatus.FINISHED,
                            "Saga finished successfully"
                    )
            );
            return;
        }

        metrics.compensationRequested();
        enqueue(
                Topics.PAYMENT,
                event,
                SagaStep.ORCHESTRATOR,
                SagaStatus.ROLLBACK,
                "Refund payment"
        );

        log.warn(
                "Inventory failed; compensation requested orderId={} correlationId={}",
                event.orderId(),
                event.correlationId()
        );
    }

    private void finish(SagaEvent event, String message) {
        outbox.enqueue(
                Topics.ORDER_NOTIFY,
                event.orderId(),
                next(
                        event,
                        SagaStep.ORCHESTRATOR,
                        SagaStatus.FAILED,
                        message
                )
        );
    }

    private void enqueue(
            String topic,
            SagaEvent event,
            SagaStep source,
            SagaStatus status,
            String message
    ) {
        outbox.enqueue(
                topic,
                event.orderId(),
                next(event, source, status, message)
        );
    }

    private SagaEvent next(
            SagaEvent event,
            SagaStep source,
            SagaStatus status,
            String message
    ) {
        return new SagaEvent(
                UUID.randomUUID().toString(),
                event.transactionId(),
                event.orderId(),
                source,
                status,
                message,
                event.payload(),
                event.correlationId(),
                Instant.now()
        );
    }

    private boolean alreadyProcessed(SagaEvent event) {
        boolean duplicate = processedEvents.existsById(event.eventId());
        if (duplicate) {
            metrics.duplicate();
            log.info(
                    "Duplicate orchestrator event ignored eventId={} correlationId={}",
                    event.eventId(),
                    event.correlationId()
            );
        }
        return duplicate;
    }

    private void markProcessed(SagaEvent event) {
        processedEvents.save(new ProcessedEvent(event.eventId(), event.correlationId()));
    }
}
