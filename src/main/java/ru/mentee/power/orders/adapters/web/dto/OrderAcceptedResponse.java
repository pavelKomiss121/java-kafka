package ru.mentee.power.orders.adapters.web.dto;

import java.time.Instant;
import java.util.UUID;

/** Ответ 202 Accepted после постановки события в Kafka. */
public class OrderAcceptedResponse {

    private UUID orderId;
    private String status;
    private Instant dispatchedAt;

    public OrderAcceptedResponse() {
    }

    public OrderAcceptedResponse(UUID orderId, String status, Instant dispatchedAt) {
        this.orderId = orderId;
        this.status = status;
        this.dispatchedAt = dispatchedAt;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getDispatchedAt() {
        return dispatchedAt;
    }

    public void setDispatchedAt(Instant dispatchedAt) {
        this.dispatchedAt = dispatchedAt;
    }
}
