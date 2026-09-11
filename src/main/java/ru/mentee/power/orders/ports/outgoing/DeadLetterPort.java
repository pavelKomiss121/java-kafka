package ru.mentee.power.orders.ports.outgoing;

/**
 * Исходящий порт: «домену нужно отправить событие, которое не удалось обработать, в DLQ».
 */
public interface DeadLetterPort {

    void publish(OrderEventPayload payload, int partition, long offset, Throwable cause);
}