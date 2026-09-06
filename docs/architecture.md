# Arquitetura

Order Service -> Kafka -> Saga Orchestrator -> Product Validation -> Payment -> Inventory. Cada serviço de domínio possui seu próprio banco. A consistência é eventual e falhas são tratadas por ações compensatórias.
