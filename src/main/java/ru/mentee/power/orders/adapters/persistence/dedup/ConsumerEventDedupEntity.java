package ru.mentee.power.orders.adapters.persistence.dedup;

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
@Table(name = "consumer_event_dedup")
@IdClass(ConsumerEventDedupEntity.Key.class)
public class ConsumerEventDedupEntity {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    protected ConsumerEventDedupEntity() {
        // для JPA
    }

    public ConsumerEventDedupEntity(UUID orderId, UUID eventId, Instant processedAt, Instant expiresAt) {
        this.orderId = orderId;
        this.eventId = eventId;
        this.processedAt = processedAt;
        this.expiresAt = expiresAt;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public static class Key implements Serializable {
        private UUID orderId;
        private UUID eventId;

        public Key() {
        }

        public Key(UUID orderId, UUID eventId) {
            this.orderId = orderId;
            this.eventId = eventId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(orderId, key.orderId) && Objects.equals(eventId, key.eventId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(orderId, eventId);
        }
    }
}