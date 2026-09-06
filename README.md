# OrderFlow — Saga Orchestration

Projeto de estudo/portfólio de microsserviços com **Java 21, Spring Boot, Apache Kafka, MongoDB, PostgreSQL e Docker**, implementando Saga Orquestrada.

## Fluxo implementado
`Order -> Orchestrator -> Product Validation -> Payment -> Inventory -> Order`

Se Inventory falhar, o Orchestrator envia compensação para Payment (refund) e encerra a Saga como FAILED.

## Módulos
- `order-service` — REST, MongoDB, inicia a Saga e recebe o resultado final.
- `saga-orchestrator` — coordena os passos.
- `product-validation-service` — valida e persiste a validação.
- `payment-service` — registra pagamento e refund.
- `inventory-service` — registra reserva de estoque.
- `event-contracts` — contratos Kafka compartilhados.

## Portas
Order 3000 | Orchestrator 8080 | Product 8090 | Payment 8091 | Inventory 8092 | Kafka 9092 | Kafka UI 8081 | Mongo 27017 | PostgreSQL 5432/5433/5434.

## Executar (modo desenvolvimento)
1. `docker compose up -d`
2. `mvn clean package`
3. Execute cada aplicação pela IDE ou com `java -jar <modulo>/target/<jar>.jar`.

## Criar pedido
```http
POST http://localhost:3000/api/orders
Content-Type: application/json

{
  "products": [
    {"code":"COMIC_BOOKS","unitValue":15.50,"quantity":3},
    {"code":"BOOKS","unitValue":9.90,"quantity":1}
  ]
}
```
A API responde `202 Accepted`. Consulte `GET /api/orders/{id}` e `GET /api/orders/sagas?orderId={id}`.

## Teste de compensação
Envie um item com `quantity > 10`; o Inventory falha, o pagamento recebe comando de refund e o pedido termina CANCELLED.

## Próximos sprints
DLT/retry, idempotência, Outbox Pattern, Testcontainers, cobertura JaCoCo mínima, tracing OpenTelemetry, Prometheus/Grafana, API Gateway, Security/JWT e Kubernetes/Terraform.
