package com.orderflow.product;

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
public class ProductListener {

    private static final Logger log = LoggerFactory.getLogger(ProductListener.class);

    private final ProductValidationRepository repository;
    private final ProcessedEventRepository processedEvents;
    private final OutboxService outbox;

    public ProductListener(
            ProductValidationRepository repository,
            ProcessedEventRepository processedEvents,
            OutboxService outbox
    ) {
        this.repository = repository;
        this.processedEvents = processedEvents;
        this.outbox = outbox;
    }

    @Transactional
    @KafkaListener(topics = Topics.PRODUCT, groupId = "product-validation")
    public void handle(SagaEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info(
                    "Duplicate Product event ignored eventId={} correlationId={}",
                    event.eventId(),
                    event.correlationId()
            );
            return;
        }

        boolean valid = event.payload().products().stream().allMatch(product ->
                product.code() != null
                        && !product.code().isBlank()
                        && product.quantity() > 0
                        && product.unitValue() != null
                        && product.unitValue().signum() > 0
        );

        var validation = new ProductValidation();
        validation.orderId = event.orderId();
        validation.status = valid ? "VALID" : "INVALID";
        repository.save(validation);

        var result = new SagaEvent(
                UUID.randomUUID().toString(),
                event.transactionId(),
                event.orderId(),
                SagaStep.PRODUCT_VALIDATION,
                valid ? SagaStatus.SUCCESS : SagaStatus.FAILED,
                valid ? "Products validated" : "Invalid product",
                event.payload(),
                event.correlationId(),
                Instant.now()
        );

        outbox.enqueue(Topics.ORCHESTRATOR, event.orderId(), result);
        processedEvents.save(new ProcessedEvent(event.eventId(), event.correlationId()));

        log.info(
                "Product validation completed orderId={} valid={} correlationId={}",
                event.orderId(),
                valid,
                event.correlationId()
        );
    }
}
