package com.orderflow.orderservice.domain;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface OrderRepository extends MongoRepository<OrderDocument, String> {

    List<OrderDocument>
    findTop100ByPendingStartEventIsNotNullAndStartEventPublishedAtIsNullOrderByCreatedAtAsc();
}
