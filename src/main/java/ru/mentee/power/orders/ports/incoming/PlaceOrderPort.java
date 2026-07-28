package ru.mentee.power.orders.ports.incoming;

import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface PlaceOrderPort {

    PlaceOrderResult place(PlaceOrderCommand command);

    record PlaceOrderCommand(
            UUID customerId,
            String region,
            OrderPriority priority,
            BigDecimal amount,
            List<OrderLine> lines
    ) {}

    record PlaceOrderResult(
            UUID orderId,
            String status,      // "QUEUED"
            Instant dispatchedAt
    ) {}
}