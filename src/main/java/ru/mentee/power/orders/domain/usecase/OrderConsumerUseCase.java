package ru.mentee.power.orders.domain.usecase;

import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.mentee.power.orders.adapters.integration.PaymentsUnavailableException;
import ru.mentee.power.orders.adapters.integration.WarehouseUnavailableException;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.adapters.metrics.IdempotencyMetricsRegistry;
import ru.mentee.power.orders.domain.saga.OrderSagaContext;
import ru.mentee.power.orders.domain.saga.OrderSagaOrchestrator;
import ru.mentee.power.orders.domain.saga.SagaStep;
import ru.mentee.power.orders.domain.saga.steps.ChargePaymentStep;
import ru.mentee.power.orders.domain.saga.steps.ConfirmOrderStep;
import ru.mentee.power.orders.domain.saga.steps.ReserveStockStep;
import ru.mentee.power.orders.ports.incoming.ProcessOrderEventPort;
import ru.mentee.power.orders.ports.outgoing.DeadLetterPort;
import ru.mentee.power.orders.ports.outgoing.DedupStorePort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class OrderConsumerUseCase implements ProcessOrderEventPort {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumerUseCase.class);

    private final OrderPersistencePort persistencePort;
    private final DeadLetterPort deadLetterPort;
    private final ConsumerMetricsRegistry metrics;
    private final DedupStorePort dedupStorePort;
    private final IdempotencyMetricsRegistry idempotencyMetrics;
    private final OrderSagaOrchestrator sagaOrchestrator;
    private final ReserveStockStep reserveStockStep;
    private final ChargePaymentStep chargePaymentStep;
    private final ConfirmOrderStep confirmOrderStep;
    private final int dedupTtlHours;

    public OrderConsumerUseCase(
            OrderPersistencePort persistencePort,
            DeadLetterPort deadLetterPort,
            ConsumerMetricsRegistry metrics,
            DedupStorePort dedupStorePort,
            IdempotencyMetricsRegistry idempotencyMetrics,
            OrderSagaOrchestrator sagaOrchestrator,
            ReserveStockStep reserveStockStep,
            ChargePaymentStep chargePaymentStep,
            ConfirmOrderStep confirmOrderStep,
            @Value("${app.idempotency.ttl-hours:48}") int dedupTtlHours
    ) {
        this.persistencePort = persistencePort;
        this.deadLetterPort = deadLetterPort;
        this.metrics = metrics;
        this.dedupStorePort = dedupStorePort;
        this.idempotencyMetrics = idempotencyMetrics;
        this.sagaOrchestrator = sagaOrchestrator;
        this.reserveStockStep = reserveStockStep;
        this.chargePaymentStep = chargePaymentStep;
        this.confirmOrderStep = confirmOrderStep;
        this.dedupTtlHours = dedupTtlHours;
    }

    @Override
    @Transactional
    public void handle(OrderEventPayload payload, int partition, long offset) {
        validate(payload);

        // guard 1 (MKAFKA-07) — TTL-ограниченная проверка конкретной доставки.
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

        // guard 2 (MKAFKA-06) — постоянная проверка терминального бизнес-факта.
        // markCancelled (MKAFKA-08) заполняет то же processedAt, что и markProcessed —
        // отменённая после компенсации сага тоже терминальна для этого guard'а (теория §2.8).
        if (persistencePort.isAlreadyProcessed(payload.orderId())) {
            metrics.duplicate();
            log.info("Duplicate order event skipped: orderId={}, partition={}, offset={}",
                    payload.orderId(), partition, offset);
            return;
        }

        // MKAFKA-08: saga вместо одного вызова pricingClient + markProcessed.
        OrderSagaContext context = new OrderSagaContext(payload, partition, offset);
        List<SagaStep<OrderSagaContext>> steps = List.of(reserveStockStep, chargePaymentStep, confirmOrderStep);

        try {
            sagaOrchestrator.run(steps, context);
        } catch (WarehouseUnavailableException | PaymentsUnavailableException ex) {
            // Временная недоступность внешнего сервиса — переиграть стоит,
            // как и PricingUnavailableException раньше (MKAFKA-05/06/07).
            metrics.dlq(payload.priority());
            deadLetterPort.publish(payload, partition, offset, ex);
            log.warn("Order routed to DLQ after saga step unavailable: orderId={}, partition={}, offset={}",
                    payload.orderId(), partition, offset);
            return;
        } catch (RuntimeException ex) {
            // Окончательный деловой отказ (OutOfStockException, PaymentDeclinedException,
            // либо неожиданная ошибка ConfirmOrderStep) — saga уже скомпенсировала
            // выполненные шаги, заказ переходит в терминальный CANCELLED (теория §2.7).
            persistencePort.markCancelled(payload.orderId(), partition, offset);
            log.warn("Order cancelled after saga compensation: orderId={}, reason={}, partition={}, offset={}",
                    payload.orderId(), ex.getMessage(), partition, offset);
            return;
        }

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
}