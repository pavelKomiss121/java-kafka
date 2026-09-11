package ru.mentee.power.orders.adapters.integration;

import java.util.UUID;

public class PaymentsUnavailableException extends RuntimeException {

    private final UUID orderId;

    public PaymentsUnavailableException(UUID orderId, Throwable cause) {
        super("Payments service unavailable after retries for order " + orderId, cause);
        this.orderId = orderId;
    }

    public UUID getOrderId() {
        return orderId;
    }
}