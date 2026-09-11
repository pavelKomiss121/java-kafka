package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;

import java.util.UUID;

public interface OrderPersistencePort {

    boolean isAlreadyProcessed(UUID orderId);

    void savePending(Order order);

    void markProcessed(Order order, int partition, long offset);

    /**
     * Терминальный статус после компенсации саги (MKAFKA-08) — заполняет то же
     * processedAt, что и markProcessed: с точки зрения guard'а isAlreadyProcessed
     * подтверждённый и отменённый заказ оба терминальны (теория §2.8).
     */
    void markCancelled(UUID orderId, int partition, long offset);
}