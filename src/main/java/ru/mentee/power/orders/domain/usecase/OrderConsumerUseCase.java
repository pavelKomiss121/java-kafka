package ru.mentee.power.orders.domain.usecase;

import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.mentee.power.orders.adapters.integration.PricingUnavailableException;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.adapters.metrics.IdempotencyMetricsRegistry;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.ports.incoming.ProcessOrderEventPort;
import ru.mentee.power.orders.ports.outgoing.DeadLetterPort;
import ru.mentee.power.orders.ports.outgoing.DedupStorePort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class OrderConsumerUseCase implements ProcessOrderEventPort {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumerUseCase.class);

    private final OrderPersistencePort persistencePort;
    private final PricingClient pricingClient;
    private final DeadLetterPort deadLetterPort;
    private final ConsumerMetricsRegistry metrics;
    private final DedupStorePort dedupStorePort;
    private final IdempotencyMetricsRegistry idempotencyMetrics;
    private final int dedupTtlHours;

    public OrderConsumerUseCase(
            OrderPersistencePort persistencePort,
            PricingClient pricingClient,
            DeadLetterPort deadLetterPort,
            ConsumerMetricsRegistry metrics,
            DedupStorePort dedupStorePort,
            IdempotencyMetricsRegistry idempotencyMetrics,
            @Value("${app.idempotency.ttl-hours:48}") int dedupTtlHours
    ) {
        this.persistencePort = persistencePort;
        this.pricingClient = pricingClient;
        this.deadLetterPort = deadLetterPort;
        this.metrics = metrics;
        this.dedupStorePort = dedupStorePort;
        this.idempotencyMetrics = idempotencyMetrics;
        this.dedupTtlHours = dedupTtlHours;
    }

    @Override
    @Transactional
    public void handle(OrderEventPayload payload, int partition, long offset) {
        validate(payload);

        // MKAFKA-07: guard 1 — TTL-ограниченная проверка КОНКРЕТНОЙ доставки.
        // tryReserve — часть этой же @Transactional-транзакции: если метод дальше
        // упадёт с непойманным исключением, резервирование откатится вместе с ним,
        // и редоставка после сбоя не будет заблокирована навсегда (теория §2.4, вопрос 4).
        Instant expiresAt = Instant.now().plus(dedupTtlHours, ChronoUnit.HOURS);
        boolean reserved = dedupStorePort.tryReserve(payload.orderId(), payload.eventId(), expiresAt);
        if (!reserved) {
            idempotencyMetrics.hit();
            metrics.duplicate();
            log.info("Redelivered event skipped by dedup guard: orderId={}, eventId={}, partition={}, offset={}",
                    payload.orderId(), payload.eventId(), partition, offset);
            return;
        }
        idempotencyMetrics.miss();

        // guard 2 — постоянная (без TTL) проверка бизнес-факта; ловит редоставку
        // ПОСЛЕ истечения TTL dedup-записи (теория §2.4).
        if (persistencePort.isAlreadyProcessed(payload.orderId())) {
            metrics.duplicate();
            log.info("Duplicate order event skipped: orderId={}, partition={}, offset={}",
                    payload.orderId(), partition, offset);
            return;
        }

        BigDecimal discount;
        try {
            discount = pricingClient.fetchDiscount(payload.orderId(), payload.region());
        } catch (PricingUnavailableException ex) {
            metrics.dlq(payload.priority());
            deadLetterPort.publish(payload, partition, offset, ex.getCause());
            log.warn("Order routed to DLQ after exhausted retries: orderId={}, partition={}, offset={}",
                    payload.orderId(), partition, offset);
            return;
        }

        Order order = toDomain(payload, discount);
        persistencePort.markProcessed(order, partition, offset);
        metrics.processed(payload.priority(), payload.region());
    }

    private void validate(OrderEventPayload payload) {
        if (payload.orderId() == null)
            throw new IllegalArgumentException("Order ID is required");
        if (payload.eventId() == null)
            throw new IllegalArgumentException("Event ID is required");
        if (payload.priority() == null)
            throw new IllegalArgumentException("Priority is required");
        if (payload.region() == null || payload.region().isBlank())
            throw new IllegalArgumentException("Region is required");
    }

    private Order toDomain(OrderEventPayload payload, BigDecimal discount) {
        List<OrderLine> lines = payload.lines().stream()
                .map(line -> {
                    OrderLine orderLine = new OrderLine();
                    orderLine.setProductId(line.productId());
                    orderLine.setQuantity(line.quantity());
                    orderLine.setPrice(line.price());
                    return orderLine;
                })
                .toList();

        BigDecimal amountAfterDiscount = payload.amount()
                .multiply(BigDecimal.ONE.subtract(discount));

        return Order.restoreFromEvent(
                payload.orderId(),
                payload.customerId(),
                payload.region(),
                payload.priority(),
                amountAfterDiscount,
                lines
        );
    }
}