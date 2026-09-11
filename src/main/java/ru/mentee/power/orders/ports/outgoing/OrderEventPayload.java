package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * eventId генерируется РОВНО ОДИН РАЗ — здесь, в from(order), вызываемом
 * из PlaceOrderUseCase.place() внутри @Transactional-границы (MKAFKA-06).
 * JpaOutboxStoreAdapter.append(...) использует это же значение как PK строки
 * outbox_event, поэтому eventId не меняется ни при одной из повторных попыток
 * публикации — см. теорию MKAFKA-07 §2.1.
 */
public record OrderEventPayload(
        UUID eventId,
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
                UUID.randomUUID(),
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