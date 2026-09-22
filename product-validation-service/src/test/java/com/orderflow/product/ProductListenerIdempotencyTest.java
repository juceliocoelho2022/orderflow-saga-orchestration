package com.orderflow.product;

import com.orderflow.contracts.OrderPayload;
import com.orderflow.contracts.SagaEvent;
import com.orderflow.contracts.SagaStatus;
import com.orderflow.contracts.SagaStep;
import com.orderflow.reliability.OutboxService;
import com.orderflow.reliability.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ProductListenerIdempotencyTest {

    @Test
    void shouldIgnoreAlreadyProcessedEventWithoutDuplicatingBusinessEffect() {
        var repository = mock(ProductValidationRepository.class);
        var processedEvents = mock(ProcessedEventRepository.class);
        var outbox = mock(OutboxService.class);

        var listener = new ProductListener(repository, processedEvents, outbox);
        var event = new SagaEvent(
                "evt-duplicate",
                "tx-1",
                "order-1",
                SagaStep.ORCHESTRATOR,
                SagaStatus.STARTED,
                "Validate products",
                mock(OrderPayload.class),
                "corr-1",
                Instant.now()
        );

        when(processedEvents.existsById("evt-duplicate")).thenReturn(true);

        listener.handle(event);

        verify(processedEvents).existsById("evt-duplicate");
        verifyNoInteractions(repository, outbox);
    }
}
