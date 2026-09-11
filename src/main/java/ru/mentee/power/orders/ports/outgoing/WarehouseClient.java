package ru.mentee.power.orders.ports.outgoing;

import java.util.UUID;

/**
 * Исходящий порт: «домену нужно зарезервировать/освободить товар на складе».
 * Реализация — WarehouseHttpClient (adapters.integration), тот же RestClient-стиль,
 * что и PricingHttpClient (MKAFKA-05).
 */
public interface WarehouseClient {

    UUID reserve(UUID orderId, UUID productId, int quantity);

    void release(UUID reservationId);
}