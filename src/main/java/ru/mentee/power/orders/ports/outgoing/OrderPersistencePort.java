package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;

import java.util.UUID;

/**
 * Исходящий порт: «домену нужно сохранить заказ и проверить, не обработан ли он уже».
 * Реализация живёт в adapters.persistence (OrderPersistenceAdapter, Spring Data JPA).
 */
public interface OrderPersistencePort {

    boolean existsById(UUID orderId);

    void save(Order order, int partition, long offset);
}