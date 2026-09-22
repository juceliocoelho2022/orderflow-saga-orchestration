package com.orderflow.orderservice.messaging;

import com.orderflow.contracts.Topics;
import com.orderflow.orderservice.domain.OrderDocument;
import com.orderflow.orderservice.domain.OrderRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class OrderOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderOutboxPublisher.class);

    private final OrderRepository orders;
    private final MongoTemplate mongoTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final long timeoutMs;

    public OrderOutboxPublisher(
            OrderRepository orders,
            MongoTemplate mongoTemplate,
            KafkaTemplate<String, Object> kafkaTemplate,
            MeterRegistry meterRegistry,
            @Value("${orderflow.reliability.publish-timeout-ms:10000}") long timeoutMs
    ) {
        this.orders = orders;
        this.mongoTemplate = mongoTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.timeoutMs = timeoutMs;
    }

    @Scheduled(fixedDelayString = "${orderflow.reliability.outbox-delay-ms:1000}")
    public void publishPendingStarts() {
        for (var order : orders
                .findTop100ByPendingStartEventIsNotNullAndStartEventPublishedAtIsNullOrderByCreatedAtAsc()) {
            try {
                var event = order.pendingStartEvent;

                kafkaTemplate.send(Topics.START, order.id, event)
                        .get(timeoutMs, TimeUnit.MILLISECONDS);

                var query = Query.query(
                        Criteria.where("_id").is(order.id)
                                .and("startEventPublishedAt").is(null)
                );
                var update = new Update()
                        .set("startEventPublishedAt", Instant.now());

                mongoTemplate.updateFirst(query, update, OrderDocument.class);

                meterRegistry.counter("orderflow.order.outbox.published").increment();

                log.info(
                        "Order Saga start published orderId={} eventId={} correlationId={}",
                        order.id,
                        event.eventId(),
                        event.correlationId()
                );
            } catch (Exception ex) {
                meterRegistry.counter("orderflow.order.outbox.publish.failures").increment();
                log.warn(
                        "Order Saga start publish failed orderId={}: {}",
                        order.id,
                        ex.getMessage()
                );
            }
        }
    }
}
