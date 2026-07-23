package ru.mentee.power.orders.adapters.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
    // TODO MKAFKA-03: при необходимости findByStatus(...)
}
