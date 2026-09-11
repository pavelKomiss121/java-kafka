package ru.mentee.power.orders.ports.outgoing;

import java.util.UUID;

/**
 * То, что домену нужно знать о событии из outbox, чтобы попытаться его опубликовать.
 * payload уже десериализован в OrderEventPayload — JSON/строки адаптер прячет внутри себя.
 */
public record OutboxEvent(
        UUID id,
        UUID orderId,
        OrderEventPayload payload,
        int attempts
) {}