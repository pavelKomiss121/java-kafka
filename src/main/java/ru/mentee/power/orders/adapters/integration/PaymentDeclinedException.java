package ru.mentee.power.orders.adapters.integration;

import java.util.UUID;

/**
 * Прямой деловой ответ платёжного шлюза (402 PAYMENT_DECLINED) — не ретраится,
 * окончательный бизнес-факт, как OutOfStockException для склада (теория §2.7).
 */
public class PaymentDeclinedException extends RuntimeException {

    private final UUID orderId;

    public PaymentDeclinedException(UUID orderId) {
        super("Payment declined for order " + orderId);
        this.orderId = orderId;
    }

    public UUID getOrderId() {
        return orderId;
    }
}