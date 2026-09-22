# Engineering Decisions — OrderFlow

Este documento registra as decisões de arquitetura do OrderFlow e diferencia capacidades atuais de evoluções planejadas.

## 1. Problema distribuído

Um pedido pode envolver validação de produto, pagamento e reserva de estoque. Cada serviço possui seu próprio estado. Uma falha posterior não pode depender de rollback ACID global.

## 2. Saga Orchestration

**Decisão:** coordenar o fluxo por um Saga Orchestrator.

**Alternativa:** choreography.

**Trade-off:** orchestration concentra conhecimento do processo no orchestrator, mas torna transições e compensações mais explícitas. Choreography reduz coordenação central, porém pode tornar fluxos grandes mais difíceis de entender.

## 3. Compensating Transactions

Quando uma etapa posterior falha, são executadas ações compensatórias nas etapas anteriores que produziram efeito.

Exemplo: pagamento aprovado + estoque indisponível gera estorno e cancelamento.

Compensação não é rollback perfeito; ela também pode falhar e precisa ser idempotente e observável.

## 4. Eventual Consistency

Os estados convergem ao longo do fluxo.

**Trade-off:** API e UI precisam representar estados intermediários, como pendente/processando, em vez de fingir uma transação instantânea global.

## 5. Kafka

Kafka desacopla produtores e consumidores e permite processamento assíncrono.

**Custo:** contratos de evento, versionamento, idempotência, retry, DLT e observabilidade de lag.

## 6. Banco por serviço

Cada serviço mantém persistência própria.

**Benefício:** menor acoplamento de schema.

**Trade-off:** joins cross-service deixam de existir e consultas agregadas exigem composição ou read models.

## 7. Semântica de entrega e confiabilidade

**Decisão:** assumir explicitamente **at-least-once delivery**.

O sistema não tenta prometer exactly-once global. Em vez disso, combina:

- `eventId` persistido para deduplicação;
- efeito de negócio + marcador idempotente + Outbox na mesma transação PostgreSQL;
- Outbox embutido no documento Mongo do pedido para tornar a criação do pedido e do evento inicial uma única escrita atômica;
- publisher que só marca a mensagem como publicada após confirmação do Kafka;
- retry limitado e DLT para falhas persistentes.

**Trade-off:** um publisher pode enviar uma mensagem e falhar antes de registrar `published_at`; isso pode gerar redelivery. Os consumidores idempotentes são a defesa deliberada contra esse cenário.

## 8. Compensação confirmada

A Saga não finaliza mais o cancelamento imediatamente após solicitar refund.

Fluxo:

1. Inventory falha;
2. Orchestrator grava comando de refund no Outbox;
3. Payment persiste `REFUNDED`;
4. Payment grava confirmação de compensação no Outbox;
5. Orchestrator recebe a confirmação;
6. somente então envia o estado final de cancelamento.

Isso torna a compensação observável e testável.

## 9. Correlation ID

`eventId`, `transactionId` e `correlationId` possuem papéis distintos.

O `correlationId` é preservado em todos os eventos derivados e incluído nos logs para diagnóstico ponta a ponta.

## 10. Atual x planejado

Implementado e testado neste incremento:

- idempotência durável;
- Transactional Outbox;
- retry/DLT;
- correlation id;
- confirmação de compensação;
- métricas Prometheus;
- CI de confiabilidade.

Ainda planejado: replay operacional de DLT, tracing OpenTelemetry e versionamento de schema com Flyway.

## 11. Estratégia de testes

Cenários prioritários:
- happy path;
- falha na validação;
- falha no pagamento;
- falha no estoque após pagamento;
- compensação;
- repetição de evento;
- indisponibilidade temporária;
- ordem inesperada de eventos.

## 12. Diagnóstico de Saga travada

1. Localizar orderId/correlation id.
2. Identificar último estado.
3. Verificar último evento.
4. Confirmar consumo no tópico seguinte.
5. Inspecionar logs do orchestrator.
6. Verificar serviço alvo.
7. Separar falha transitória de permanente.
8. Decidir retry, compensação ou intervenção.
9. Garantir replay idempotente.

## 13. Quando usar Saga

Saga faz sentido com múltiplos serviços, estados independentes e compensações possíveis.

Para um domínio simples em um único banco, uma transação local é mais simples e normalmente preferível.
