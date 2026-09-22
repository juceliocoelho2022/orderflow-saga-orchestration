# OrderFlow Reliability Hardening

## Objective

Move the project from a functional Saga demonstration to a flow that explicitly handles redelivery, partial failure and compensation.

## Delivery semantics

OrderFlow uses **at-least-once delivery**.

Exactly-once processing is not claimed. Reliability comes from:

1. durable idempotency markers;
2. transactional database + Outbox writes;
3. bounded retry;
4. DLT isolation;
5. replay-safe consumers;
6. correlation IDs propagated across the Saga.

## Idempotent consumers

The PostgreSQL-backed participants and Saga Orchestrator persist each consumed `eventId` in `processed_events`.

Business state, the processed-event marker and the outgoing Outbox row are written in the **same database transaction**.

If Kafka redelivers the same event after the database commit, the consumer sees the existing marker and does not apply the business effect twice.

The Order Service final notification stores processed event IDs in Saga History. Order status updates are naturally idempotent because the final state is assigned rather than incremented.

## Transactional Outbox

### PostgreSQL participants

Product Validation, Payment, Inventory and Saga Orchestrator use `outbox_messages`.

Within the same transaction:

```text
business state
+ processed event
+ outgoing SagaEvent
= COMMIT
```

A scheduler publishes pending rows to Kafka and marks them as published only after Kafka confirms the send.

A crash after Kafka accepts a message but before `published_at` is stored can cause a duplicate. This is why downstream idempotency remains required.

### Mongo Order Service

The initial Saga event is embedded in the same Mongo `orders` document as `pendingStartEvent`.

Because a single Mongo document write is atomic, order creation and creation of the pending Saga-start message cannot diverge.

The scheduler publishes the embedded event and updates only `startEventPublishedAt` using `MongoTemplate`, avoiding a stale full-document overwrite.

## Retry and DLT

Consumer exceptions use:

- fixed backoff: 1 second;
- max retries: 2;
- after retry exhaustion: `<source-topic>.DLT`.

Metrics:

- `orderflow_kafka_retry_attempts_total`
- `orderflow_kafka_dlt_published_total`

Permanent failures are therefore isolated instead of blocking a consumer indefinitely.

## Compensation

The compensation flow now waits for acknowledgement:

```text
Inventory FAILED
      |
      v
Refund command -> Payment
      |
      v
Payment status = REFUNDED
      |
      v
Payment emits ROLLBACK confirmation
      |
      v
Saga Orchestrator marks compensation completed
      |
      v
Order CANCELLED
```

The order is no longer cancelled immediately after merely requesting the refund.

## Correlation

Each order has:

- `transactionId`: Saga transaction identity;
- `correlationId`: end-to-end diagnostic identity;
- `eventId`: idempotency identity for one message.

Every derived Saga event preserves `correlationId`, and service logs include it.

## Saga metrics

The Orchestrator exposes:

- `orderflow_saga_completed_total`
- `orderflow_saga_failed_total`
- `orderflow_saga_compensation_requested_total`
- `orderflow_saga_compensated_total`
- `orderflow_saga_duplicate_events_total`

Outbox metrics:

- `orderflow_outbox_published_total`
- `orderflow_outbox_publish_failures_total`

Prometheus scrapes all five services through `/actuator/prometheus`.

## Automated evidence

Tests cover:

- duplicate Product event -> no duplicate repository write/outbox message;
- Payment compensation -> persisted `REFUNDED` + compensation confirmation;
- Inventory failure -> refund requested but order is not cancelled yet;
- refund confirmation -> final cancellation event emitted.

CI runs:

```bash
mvn clean verify
docker compose -f docker-compose.yml -f compose-apps.yml config --quiet
promtool check config observability/prometheus.yml
```

## Known boundaries

- DLT replay tooling is not yet automated.
- No claim of Kafka exactly-once semantics is made.
- PostgreSQL tables currently use Hibernate `ddl-auto=update`; Flyway would be the next schema-hardening step.
- Distributed tracing is not yet implemented; correlation IDs provide log-level traceability in this increment.
