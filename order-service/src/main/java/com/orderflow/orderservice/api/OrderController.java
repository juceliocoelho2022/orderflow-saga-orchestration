package com.orderflow.orderservice.api;

import com.orderflow.contracts.OrderPayload;
import com.orderflow.contracts.ProductItem;
import com.orderflow.contracts.SagaEvent;
import com.orderflow.contracts.SagaStatus;
import com.orderflow.contracts.SagaStep;
import com.orderflow.orderservice.domain.OrderDocument;
import com.orderflow.orderservice.domain.OrderRepository;
import com.orderflow.orderservice.domain.SagaHistory;
import com.orderflow.orderservice.domain.SagaHistoryRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderRepository orders;
    private final SagaHistoryRepository history;

    public OrderController(
            OrderRepository orders,
            SagaHistoryRepository history
    ) {
        this.orders = orders;
        this.history = history;
    }

    @PostMapping
    public ResponseEntity<OrderDocument> create(@RequestBody CreateOrderRequest request) {
        if (request.products() == null || request.products().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }

        var order = new OrderDocument();
        order.id = UUID.randomUUID().toString();
        order.transactionId = UUID.randomUUID().toString();
        order.correlationId = UUID.randomUUID().toString();
        order.products = request.products();
        order.status = "PENDING";
        order.createdAt = Instant.now();

        BigDecimal total = order.products.stream()
                .map(product -> product.unitValue()
                        .multiply(BigDecimal.valueOf(product.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        int items = order.products.stream()
                .mapToInt(ProductItem::quantity)
                .sum();

        var payload = new OrderPayload(
                order.id,
                order.products,
                total,
                items
        );

        order.pendingStartEvent = new SagaEvent(
                UUID.randomUUID().toString(),
                order.transactionId,
                order.id,
                SagaStep.ORDER,
                SagaStatus.STARTED,
                "Saga started",
                payload,
                order.correlationId,
                Instant.now()
        );

        order = orders.save(order);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(order);
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderDocument> get(@PathVariable String id) {
        return ResponseEntity.of(orders.findById(id));
    }

    @GetMapping("/sagas")
    public ResponseEntity<SagaHistory> saga(
            @RequestParam(required = false) String orderId,
            @RequestParam(required = false) String transactionId
    ) {
        Optional<SagaHistory> result =
                orderId != null
                        ? history.findByOrderId(orderId)
                        : transactionId != null
                        ? history.findByTransactionId(transactionId)
                        : Optional.empty();

        return ResponseEntity.of(result);
    }
}
