package ru.mentee.power.orders.adapters.persistence.saga;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaExecutionLogRepository
        extends JpaRepository<SagaExecutionLogEntity, SagaExecutionLogEntity.Key> {
}