package com.orderflow.reliability;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxMessageRepository extends JpaRepository<OutboxMessage, String> {
    List<OutboxMessage> findTop100ByPublishedAtIsNullOrderByCreatedAtAsc();
}
