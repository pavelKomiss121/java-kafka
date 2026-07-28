package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderEventPayload(
        UUID orderId,
        UUID customerId,
        String region,
        BigDecimal amount,
        OrderPriority priority,
        List<Line> lines,
        Instant emittedAt
) {
    public record Line(UUID productId, int quantity, BigDecimal price) {}

    public static OrderEventPayload from(Order order) {
        List<Line> lines = order.getLines().stream()
                .map(l -> new Line(l.getProductId(), l.getQuantity(), l.getPrice()))
                .toList();
        return new OrderEventPayload(
                order.getId(),
                order.getCustomerId(),
                order.getRegion(),
                order.getAmount(),
                order.getPriority(),
                lines,
                Instant.now()
        );
    }
}