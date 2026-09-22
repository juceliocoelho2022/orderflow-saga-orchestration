package com.orderflow.payment;

import com.orderflow.contracts.OrderPayload;
import com.orderflow.contracts.SagaEvent;
import com.orderflow.contracts.SagaStatus;
import com.orderflow.contracts.SagaStep;
import com.orderflow.contracts.Topics;
import com.orderflow.reliability.OutboxService;
import com.orderflow.reliability.ProcessedEventRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaymentListenerCompensationTest {

    @Test
    void shouldRefundPaymentAndPublishCompensationConfirmation() {
        var repository = mock(PaymentRepository.class);
        var processedEvents = mock(ProcessedEventRepository.class);
        var outbox = mock(OutboxService.class);

        var payment = new Payment();
        payment.id = 10L;
        payment.orderId = "order-1";
        payment.amount = new BigDecimal("150.00");
        payment.status = "PAID";

        when(processedEvents.existsById("evt-refund")).thenReturn(false);
        when(repository.findFirstByOrderIdOrderByIdDesc("order-1"))
                .thenReturn(Optional.of(payment));

        var listener = new PaymentListener(repository, processedEvents, outbox);
        var event = new SagaEvent(
                "evt-refund",
                "tx-1",
                "order-1",
                SagaStep.ORCHESTRATOR,
                SagaStatus.ROLLBACK,
                "Refund payment",
                mock(OrderPayload.class),
                "corr-1",
                Instant.now()
        );

        listener.handle(event);

        assertThat(payment.status).isEqualTo("REFUNDED");
        verify(repository).save(payment);

        var captor = ArgumentCaptor.forClass(SagaEvent.class);
        verify(outbox).enqueue(eq(Topics.ORCHESTRATOR), eq("order-1"), captor.capture());

        assertThat(captor.getValue().source()).isEqualTo(SagaStep.PAYMENT);
        assertThat(captor.getValue().status()).isEqualTo(SagaStatus.ROLLBACK);
        assertThat(captor.getValue().correlationId()).isEqualTo("corr-1");
    }
}
