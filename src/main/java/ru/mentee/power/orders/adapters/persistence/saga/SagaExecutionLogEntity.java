package ru.mentee.power.orders.adapters.persistence.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Entity
@Table(name = "saga_execution_log")
@IdClass(SagaExecutionLogEntity.Key.class)
public class SagaExecutionLogEntity {

    @Id
    @Column(name = "saga_id")
    private UUID sagaId;

    @Id
    @Column(name = "step_name")
    private String stepName;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected SagaExecutionLogEntity() {
        // для JPA
    }

    public SagaExecutionLogEntity(UUID sagaId, String stepName, UUID orderId, String status, Instant now) {
        this.sagaId = sagaId;
        this.stepName = stepName;
        this.orderId = orderId;
        this.status = status;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public String getStepName() {
        return stepName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public static class Key implements Serializable {
        private UUID sagaId;
        private String stepName;

        public Key() {
        }

        public Key(UUID sagaId, String stepName) {
            this.sagaId = sagaId;
            this.stepName = stepName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(sagaId, key.sagaId) && Objects.equals(stepName, key.stepName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sagaId, stepName);
        }
    }
}