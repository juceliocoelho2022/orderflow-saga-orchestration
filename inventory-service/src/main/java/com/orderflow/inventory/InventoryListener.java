package com.orderflow.inventory;

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
public class InventoryListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryListener.class);

    private final InventoryRepository repository;
    private final ProcessedEventRepository processedEvents;
    private final OutboxService outbox;

    public InventoryListener(
            InventoryRepository repository,
            ProcessedEventRepository processedEvents,
            OutboxService outbox
    ) {
        this.repository = repository;
        this.processedEvents = processedEvents;
        this.outbox = outbox;
    }

    @Transactional
    @KafkaListener(topics = Topics.INVENTORY, groupId = "inventory")
    public void handle(SagaEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info(
                    "Duplicate Inventory event ignored eventId={} correlationId={}",
                    event.eventId(),
                    event.correlationId()
            );
            return;
        }

        boolean reserved = event.payload().products().stream()
                .allMatch(product -> product.quantity() <= 10);

        var reservation = new InventoryReservation();
        reservation.orderId = event.orderId();
        reservation.status = reserved ? "RESERVED" : "FAILED";
        repository.save(reservation);

        var result = new SagaEvent(
                UUID.randomUUID().toString(),
                event.transactionId(),
                event.orderId(),
                SagaStep.INVENTORY,
                reserved ? SagaStatus.SUCCESS : SagaStatus.FAILED,
                reserved ? "Inventory reserved" : "Insufficient inventory",
                event.payload(),
                event.correlationId(),
                Instant.now()
        );

        outbox.enqueue(Topics.ORCHESTRATOR, event.orderId(), result);
        processedEvents.save(new ProcessedEvent(event.eventId(), event.correlationId()));

        log.info(
                "Inventory processed orderId={} reserved={} correlationId={}",
                event.orderId(),
                reserved,
                event.correlationId()
        );
    }
}
