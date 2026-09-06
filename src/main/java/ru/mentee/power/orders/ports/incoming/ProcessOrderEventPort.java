package ru.mentee.power.orders.ports.incoming;

import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;

/**
 * Входящий порт: «домену нужно обработать событие заказа, прочитанное из Kafka».
 * Его вызывает адаптер-listener. Его реализует use case консьюмера.
 * Переиспользует {@link OrderEventPayload} продюсера — это то же самое событие,
 * которое улетело в топик в MKAFKA-03, просто теперь оно read, а не write.
 */
public interface ProcessOrderEventPort {

    void handle(OrderEventPayload payload, int partition, long offset);
}