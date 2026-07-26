package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.OrderPriority;

import java.util.concurrent.CompletionStage;

/**
 * Исходящий порт: «домену нужно опубликовать событие».
 * Реализация живёт в adapters.kafka (OrderEventProducer).
 */
public interface OrderEventPort {

    CompletionStage<Void> publish(OrderEventPayload payload, OrderPriority priority);
}
