package ru.mentee.power.orders.adapters.integration;

import java.util.UUID;

/**
 * Прямой деловой ответ склада (409 OUT_OF_STOCK) — НЕ ретраится Resilience4j
 * (в отличие от ExternalServiceException), обрабатывается сагой немедленно
 * как окончательный бизнес-факт (теория §2.7).
 */
public class OutOfStockException extends RuntimeException {

    private final UUID orderId;

    public OutOfStockException(UUID orderId, UUID productId) {
        super("Product " + productId + " out of stock for order " + orderId);
        this.orderId = orderId;
    }

    public UUID getOrderId() {
        return orderId;
    }
}