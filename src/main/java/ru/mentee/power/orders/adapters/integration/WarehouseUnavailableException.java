package ru.mentee.power.orders.adapters.integration;

import java.util.UUID;

/**
 * Бросается из fallback-метода WarehouseHttpClient, когда все попытки retry
 * исчерпаны — временная недоступность склада. OrderConsumerUseCase ловит
 * именно этот тип, чтобы направить событие в DLQ, а не компенсировать сагу.
 */
public class WarehouseUnavailableException extends RuntimeException {

    private final UUID orderId;

    public WarehouseUnavailableException(UUID orderId, Throwable cause) {
        super("Warehouse service unavailable after retries for order " + orderId, cause);
        this.orderId = orderId;
    }

    public UUID getOrderId() {
        return orderId;
    }
}