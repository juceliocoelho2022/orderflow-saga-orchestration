package com.orderflow.orchestrator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class SagaMetrics {

    private final Counter completed;
    private final Counter failed;
    private final Counter compensationRequested;
    private final Counter compensated;
    private final Counter duplicates;

    public SagaMetrics(MeterRegistry meterRegistry) {
        completed = meterRegistry.counter("orderflow.saga.completed");
        failed = meterRegistry.counter("orderflow.saga.failed");
        compensationRequested = meterRegistry.counter("orderflow.saga.compensation.requested");
        compensated = meterRegistry.counter("orderflow.saga.compensated");
        duplicates = meterRegistry.counter("orderflow.saga.duplicate.events");
    }

    public void completed() {
        completed.increment();
    }

    public void failed() {
        failed.increment();
    }

    public void compensationRequested() {
        compensationRequested.increment();
    }

    public void compensated() {
        compensated.increment();
    }

    public void duplicate() {
        duplicates.increment();
    }
}
