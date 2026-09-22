package com.orderflow.orderservice.messaging;

import com.orderflow.contracts.SagaEvent;
import com.orderflow.contracts.SagaStatus;
import com.orderflow.contracts.Topics;
import com.orderflow.orderservice.domain.OrderRepository;
import com.orderflow.orderservice.domain.SagaHistory;
import com.orderflow.orderservice.domain.SagaHistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class OrderSagaListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaListener.class);

    private final OrderRepository orders;
    private final SagaHistoryRepository history;

    public OrderSagaListener(
            OrderRepository orders,
            SagaHistoryRepository history
    ) {
        this.orders = orders;
        this.history = history;
    }

    @KafkaListener(topics = Topics.ORDER_NOTIFY, groupId = "order-service")
    public void on(SagaEvent event) {
        var saga = history.findByOrderId(event.orderId())
                .orElseGet(SagaHistory::new);

        if (saga.processedEventIds.contains(event.eventId())) {
            log.info(
                    "Duplicate final Saga event ignored eventId={} correlationId={}",
                    event.eventId(),
                    event.correlationId()
            );
            return;
        }

        orders.findById(event.orderId()).ifPresent(order -> {
            order.status =
                    event.status() == SagaStatus.FINISHED
                            ? "COMPLETED"
                            : "CANCELLED";
            orders.save(order);
        });

        saga.orderId = event.orderId();
        saga.transactionId = event.transactionId();
        saga.correlationId = event.correlationId();
        saga.status = event.status().name();
        saga.events.add(
                event.createdAt()
                        + " | correlationId=" + event.correlationId()
                        + " | " + event.source()
                        + " | " + event.status()
                        + " | " + event.message()
        );
        saga.processedEventIds.add(event.eventId());
        saga.updatedAt = Instant.now();
        history.save(saga);

        log.info(
                "Order Saga finalized orderId={} status={} correlationId={}",
                event.orderId(),
                event.status(),
                event.correlationId()
        );
    }
}
