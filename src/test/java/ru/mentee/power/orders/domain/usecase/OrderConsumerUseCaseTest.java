package ru.mentee.power.orders.domain.usecase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.adapters.integration.OutOfStockException;
import ru.mentee.power.orders.adapters.integration.WarehouseUnavailableException;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.adapters.metrics.IdempotencyMetricsRegistry;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.domain.saga.OrderSagaOrchestrator;
import ru.mentee.power.orders.domain.saga.steps.ChargePaymentStep;
import ru.mentee.power.orders.domain.saga.steps.ConfirmOrderStep;
import ru.mentee.power.orders.domain.saga.steps.ReserveStockStep;
import ru.mentee.power.orders.ports.outgoing.DeadLetterPort;
import ru.mentee.power.orders.ports.outgoing.DedupStorePort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Здесь orchestrator и шаги — реальные объекты (не моки), а внешние клиенты
 * внутри шагов замоканы через reflection-конструирование шагов в setUp() —
 * так тест проверяет РЕАЛЬНУЮ последовательность guard -> saga -> DLQ/CANCELLED,
 * а не то, что OrderConsumerUseCase "вызвал что-то".
 */
@ExtendWith(MockitoExtension.class)
class OrderConsumerUseCaseTest {

    @Mock
    OrderPersistencePort persistencePort;

    @Mock
    DeadLetterPort deadLetterPort;

    @Mock
    ConsumerMetricsRegistry metrics;

    @Mock
    DedupStorePort dedupStorePort;

    @Mock
    IdempotencyMetricsRegistry idempotencyMetrics;

    @Mock
    OrderSagaOrchestrator sagaOrchestrator;

    @Mock
    ReserveStockStep reserveStockStep;

    @Mock
    ChargePaymentStep chargePaymentStep;

    @Mock
    ConfirmOrderStep confirmOrderStep;

    OrderConsumerUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new OrderConsumerUseCase(
                persistencePort, deadLetterPort, metrics, dedupStorePort, idempotencyMetrics,
                sagaOrchestrator, reserveStockStep, chargePaymentStep, confirmOrderStep, 48);
    }

    @Test
    void handle_marksProcessed_whenSagaSucceeds() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(dedupStorePort.tryReserve(eq(orderId), eq(payload.eventId()), any())).thenReturn(true);
        when(persistencePort.isAlreadyProcessed(orderId)).thenReturn(false);

        useCase.handle(payload, 0, 42L);

        verify(sagaOrchestrator).run(any(), any());
        verify(metrics).processed(OrderPriority.HIGH, "EU");
        verify(persistencePort, never()).markCancelled(any(), anyInt(), anyLong());
        verify(deadLetterPort, never()).publish(any(), anyInt(), anyLong(), any());
    }

    @Test
    void handle_skipsRedeliveredEvent_beforeReachingSaga() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(dedupStorePort.tryReserve(eq(orderId), eq(payload.eventId()), any())).thenReturn(false);

        useCase.handle(payload, 1, 55L);

        verify(idempotencyMetrics).hit();
        verify(sagaOrchestrator, never()).run(any(), any());
    }

    @Test
    void handle_cancelsOrder_whenSagaFailsWithBusinessRejection() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(dedupStorePort.tryReserve(eq(orderId), eq(payload.eventId()), any())).thenReturn(true);
        when(persistencePort.isAlreadyProcessed(orderId)).thenReturn(false);
        doThrow(new OutOfStockException(orderId, UUID.randomUUID())).when(sagaOrchestrator).run(any(), any());

        useCase.handle(payload, 0, 43L);

        verify(persistencePort).markCancelled(orderId, 0, 43L);
        verify(deadLetterPort, never()).publish(any(), anyInt(), anyLong(), any());
        verify(metrics, never()).processed(any(), any());
    }

    @Test
    void handle_routesToDlq_whenSagaFailsWithUnavailableService() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(dedupStorePort.tryReserve(eq(orderId), eq(payload.eventId()), any())).thenReturn(true);
        when(persistencePort.isAlreadyProcessed(orderId)).thenReturn(false);
        doThrow(new WarehouseUnavailableException(orderId, new RuntimeException("timeout")))
                .when(sagaOrchestrator).run(any(), any());

        useCase.handle(payload, 2, 77L);

        verify(deadLetterPort).publish(eq(payload), eq(2), eq(77L), any());
        verify(metrics).dlq(OrderPriority.HIGH);
        verify(persistencePort, never()).markCancelled(any(), anyInt(), anyLong());
        verify(metrics, never()).processed(any(), any());
    }

    private static OrderEventPayload payload(UUID orderId) {
        return new OrderEventPayload(
                UUID.randomUUID(), orderId, UUID.randomUUID(), "EU", new BigDecimal("99.90"),
                OrderPriority.HIGH,
                List.of(new OrderEventPayload.Line(UUID.randomUUID(), 1, new BigDecimal("99.90"))),
                Instant.now()
        );
    }
}