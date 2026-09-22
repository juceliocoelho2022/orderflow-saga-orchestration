package com.orderflow.orchestrator;

import com.orderflow.contracts.OrderPayload;
import com.orderflow.contracts.SagaEvent;
import com.orderflow.contracts.SagaStatus;
import com.orderflow.contracts.SagaStep;
import com.orderflow.contracts.Topics;
import com.orderflow.reliability.OutboxService;
import com.orderflow.reliability.ProcessedEventRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SagaOrchestratorCompensationTest {

    @Test
    void shouldWaitForRefundConfirmationBeforeCancellingOrder() {
        var processedEvents = mock(ProcessedEventRepository.class);
        var outbox = mock(OutboxService.class);
        var metrics = mock(SagaMetrics.class);
        var orchestrator = new SagaOrchestrator(processedEvents, outbox, metrics);

        var payload = mock(OrderPayload.class);
        var inventoryFailure = new SagaEvent(
                "evt-inventory-failed",
                "tx-1",
                "order-1",
                SagaStep.INVENTORY,
                SagaStatus.FAILED,
                "Insufficient inventory",
                payload,
                "corr-1",
                Instant.now()
        );

        when(processedEvents.existsById("evt-inventory-failed")).thenReturn(false);

        orchestrator.result(inventoryFailure);

        verify(outbox).enqueue(eq(Topics.PAYMENT), eq("order-1"), any(SagaEvent.class));
        verify(outbox, never()).enqueue(eq(Topics.ORDER_NOTIFY), eq("order-1"), any(SagaEvent.class));
        verify(metrics).compensationRequested();

        clearInvocations(outbox, metrics);

        var refundConfirmation = new SagaEvent(
                "evt-refund-confirmed",
                "tx-1",
                "order-1",
                SagaStep.PAYMENT,
                SagaStatus.ROLLBACK,
                "Payment refunded",
                payload,
                "corr-1",
                Instant.now()
        );

        when(processedEvents.existsById("evt-refund-confirmed")).thenReturn(false);

        orchestrator.result(refundConfirmation);

        verify(outbox).enqueue(eq(Topics.ORDER_NOTIFY), eq("order-1"), any(SagaEvent.class));
        verify(metrics).compensated();
    }
}
