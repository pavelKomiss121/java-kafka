package ru.mentee.power.orders.domain.usecase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.adapters.metrics.OutboxMetricsRegistry;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.incoming.DispatchOutboxPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;
import ru.mentee.power.orders.ports.outgoing.OutboxEvent;
import ru.mentee.power.orders.ports.outgoing.OutboxStorePort;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxDispatchUseCaseTest {

    @Mock
    OutboxStorePort outboxStorePort;

    @Mock
    OrderEventPort orderEventPort;

    @Mock
    OutboxMetricsRegistry metrics;

    OutboxDispatchUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new OutboxDispatchUseCase(outboxStorePort, orderEventPort, metrics, 3);
    }

    @Test
    void dispatchDueBatch_marksSent_whenPublishSucceeds() {
        OutboxEvent event = event(0);
        when(outboxStorePort.fetchDueBatch(10)).thenReturn(List.of(event));
        when(orderEventPort.publish(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

        DispatchOutboxPort.DispatchResult result = useCase.dispatchDueBatch(10);

        assertEquals(1, result.sent());
        assertEquals(0, result.failed());
        assertEquals(0, result.dead());
        verify(outboxStorePort).markSent(event.id());
        verify(outboxStorePort, never()).markFailed(any(), anyString(), any());
        verify(outboxStorePort, never()).markDead(any(), anyString());
    }

    @Test
    void dispatchDueBatch_marksFailed_whenAttemptsBelowLimit() {
        OutboxEvent event = event(0);
        when(outboxStorePort.fetchDueBatch(10)).thenReturn(List.of(event));
        when(orderEventPort.publish(any(), any())).thenReturn(failedFuture());

        DispatchOutboxPort.DispatchResult result = useCase.dispatchDueBatch(10);

        assertEquals(0, result.sent());
        assertEquals(1, result.failed());
        assertEquals(0, result.dead());
        verify(outboxStorePort).markFailed(eq(event.id()), anyString(), any(Instant.class));
        verify(outboxStorePort, never()).markDead(any(), anyString());
    }

    @Test
    void dispatchDueBatch_marksDead_whenAttemptsExhausted() {
        OutboxEvent event = event(2);
        when(outboxStorePort.fetchDueBatch(10)).thenReturn(List.of(event));
        when(orderEventPort.publish(any(), any())).thenReturn(failedFuture());

        DispatchOutboxPort.DispatchResult result = useCase.dispatchDueBatch(10);

        assertEquals(0, result.sent());
        assertEquals(0, result.failed());
        assertEquals(1, result.dead());
        verify(outboxStorePort).markDead(eq(event.id()), anyString());
        verify(outboxStorePort, never()).markFailed(any(), anyString(), any());
    }

    private static CompletableFuture<Void> failedFuture() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("broker unavailable"));
        return future;
    }

    private static OutboxEvent event(int attempts) {
        OrderEventPayload payload = new OrderEventPayload(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), "EU", new BigDecimal("10.00"),
                OrderPriority.HIGH, List.of(), Instant.now()
        );
        return new OutboxEvent(UUID.randomUUID(), payload.orderId(), payload, attempts);
    }
}