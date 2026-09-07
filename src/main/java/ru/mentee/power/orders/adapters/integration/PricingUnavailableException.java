package ru.mentee.power.orders.adapters.integration;

import java.util.UUID;

/**
 * Бросается из fallback-метода PricingHttpClient, когда все попытки retry исчерпаны.
 * OrderConsumerUseCase ловит именно этот тип, чтобы направить событие в DLQ.
 */
public class PricingUnavailableException extends RuntimeException {

    private final UUID orderId;

    public PricingUnavailableException(UUID orderId, Throwable cause) {
        super("Pricing service unavailable after retries for order " + orderId, cause);
        this.orderId = orderId;
    }

    public UUID getOrderId() {
        return orderId;
    }
}
