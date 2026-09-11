package ru.mentee.power.orders.domain.saga;

import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Изменяемый контекст одной саги: шаги пишут в него данные (reservationId,
 * paymentId, discount), которые нужны либо следующему шагу, либо собственной
 * компенсации того же шага (теория §2.3).
 */
public class OrderSagaContext {

    private final UUID orderId;
    private final UUID sagaId;
    private final OrderEventPayload payload;
    private final int partition;
    private final long offset;

    private BigDecimal discount;
    private UUID reservationId;
    private UUID paymentId;

    public OrderSagaContext(OrderEventPayload payload, int partition, long offset) {
        this.orderId = payload.orderId();
        this.sagaId = payload.eventId();
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }

    public UUID orderId() {
        return orderId;
    }

    public UUID sagaId() {
        return sagaId;
    }

    public OrderEventPayload payload() {
        return payload;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public BigDecimal discount() {
        return discount;
    }

    public void setDiscount(BigDecimal discount) {
        this.discount = discount;
    }

    public UUID reservationId() {
        return reservationId;
    }

    public void setReservationId(UUID reservationId) {
        this.reservationId = reservationId;
    }

    public UUID paymentId() {
        return paymentId;
    }

    public void setPaymentId(UUID paymentId) {
        this.paymentId = paymentId;
    }
}