package com.orderflow.payment;

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
public class PaymentListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final PaymentRepository repository;
    private final ProcessedEventRepository processedEvents;
    private final OutboxService outbox;

    public PaymentListener(
            PaymentRepository repository,
            ProcessedEventRepository processedEvents,
            OutboxService outbox
    ) {
        this.repository = repository;
        this.processedEvents = processedEvents;
        this.outbox = outbox;
    }

    @Transactional
    @KafkaListener(topics = Topics.PAYMENT, groupId = "payment")
    public void handle(SagaEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info(
                    "Duplicate Payment event ignored eventId={} correlationId={}",
                    event.eventId(),
                    event.correlationId()
            );
            return;
        }

        if (event.status() == SagaStatus.ROLLBACK) {
            refund(event);
            return;
        }

        boolean paid = event.payload().totalAmount().signum() > 0;

        var payment = new Payment();
        payment.orderId = event.orderId();
        payment.amount = event.payload().totalAmount();
        payment.status = paid ? "PAID" : "FAILED";
        repository.save(payment);

        var result = new SagaEvent(
                UUID.randomUUID().toString(),
                event.transactionId(),
                event.orderId(),
                SagaStep.PAYMENT,
                paid ? SagaStatus.SUCCESS : SagaStatus.FAILED,
                paid ? "Payment processed" : "Payment failed",
                event.payload(),
                event.correlationId(),
                Instant.now()
        );

        outbox.enqueue(Topics.ORCHESTRATOR, event.orderId(), result);
        processedEvents.save(new ProcessedEvent(event.eventId(), event.correlationId()));

        log.info(
                "Payment processed orderId={} paid={} correlationId={}",
                event.orderId(),
                paid,
                event.correlationId()
        );
    }

    private void refund(SagaEvent event) {
        var payment = repository.findFirstByOrderIdOrderByIdDesc(event.orderId())
                .orElseThrow(() -> new IllegalStateException(
                        "Payment not found for compensation: " + event.orderId()
                ));

        payment.status = "REFUNDED";
        repository.save(payment);

        var compensationResult = new SagaEvent(
                UUID.randomUUID().toString(),
                event.transactionId(),
                event.orderId(),
                SagaStep.PAYMENT,
                SagaStatus.ROLLBACK,
                "Payment refunded",
                event.payload(),
                event.correlationId(),
                Instant.now()
        );

        outbox.enqueue(Topics.ORCHESTRATOR, event.orderId(), compensationResult);
        processedEvents.save(new ProcessedEvent(event.eventId(), event.correlationId()));

        log.info(
                "Payment refunded orderId={} correlationId={}",
                event.orderId(),
                event.correlationId()
        );
    }
}
