package ru.mentee.power.orders.adapters.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OrderLineRepository extends JpaRepository<OrderLineEntity, UUID> {
}
