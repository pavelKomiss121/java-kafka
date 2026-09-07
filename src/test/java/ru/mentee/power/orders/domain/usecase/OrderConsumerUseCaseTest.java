package ru.mentee.power.orders.domain.usecase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.adapters.integration.PricingUnavailableException;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.outgoing.DeadLetterPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderConsumerUseCaseTest {

    @Mock
    OrderPersistencePort persistencePort;

    @Mock
    PricingClient pricingClient;

    @Mock
    DeadLetterPort deadLetterPort;

    @Mock
    ConsumerMetricsRegistry metrics;

    OrderConsumerUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new OrderConsumerUseCase(persistencePort, pricingClient, deadLetterPort, metrics);
    }

    @Test
    void handle_savesNewOrder_andRecordsMetric() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(persistencePort.existsById(orderId)).thenReturn(false);
        when(pricingClient.fetchDiscount(orderId, "EU")).thenReturn(BigDecimal.ZERO);

        useCase.handle(payload, 0, 42L);

        verify(persistencePort).save(any(), anyInt(), anyLong());
        verify(metrics).processed(OrderPriority.HIGH, "EU");
        verify(metrics, never()).duplicate();
        verify(metrics, never()).dlq(any());
        verifyNoInteractions(deadLetterPort);
    }

    @Test
    void handle_appliesDiscountToSavedAmount() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(persistencePort.existsById(orderId)).thenReturn(false);
        when(pricingClient.fetchDiscount(orderId, "EU")).thenReturn(new BigDecimal("0.05"));

        useCase.handle(payload, 0, 42L);

        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(persistencePort).save(orderCaptor.capture(), anyInt(), anyLong());
        assertEquals(0, new BigDecimal("94.905").compareTo(orderCaptor.getValue().getAmount()));
    }

    @Test
    void handle_skipsDuplicate() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        when(persistencePort.existsById(orderId)).thenReturn(true);

        useCase.handle(payload, 0, 43L);

        verify(persistencePort, never()).save(any(), anyInt(), anyLong());
        verify(metrics).duplicate();
        verifyNoInteractions(pricingClient);
        verifyNoInteractions(deadLetterPort);
    }

    @Test
    void handle_rejectsMissingRegion() {
        OrderEventPayload payload = new OrderEventPayload(
                UUID.randomUUID(), UUID.randomUUID(), " ", new BigDecimal("10.00"),
                OrderPriority.LOW, List.of(line()), Instant.now()
        );

        assertThrows(IllegalArgumentException.class, () -> useCase.handle(payload, 0, 1L));
        verify(persistencePort, never()).existsById(any());
        verifyNoInteractions(pricingClient);
        verifyNoInteractions(deadLetterPort);
    }

    @Test
    void handle_routesToDlqWhenPricingUnavailableAfterRetries() {
        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId);
        RuntimeException cause = new RuntimeException("pricing service down");
        when(persistencePort.existsById(orderId)).thenReturn(false);
        when(pricingClient.fetchDiscount(orderId, "EU"))
                .thenThrow(new PricingUnavailableException(orderId, cause));

        useCase.handle(payload, 2, 77L);

        verify(deadLetterPort).publish(eq(payload), eq(2), eq(77L), eq(cause));
        verify(metrics).dlq(OrderPriority.HIGH);
        verify(persistencePort, never()).save(any(), anyInt(), anyLong());
        verify(metrics, never()).processed(any(), any());
    }

    private static OrderEventPayload payload(UUID orderId) {
        return new OrderEventPayload(
                orderId, UUID.randomUUID(), "EU", new BigDecimal("99.90"),
                OrderPriority.HIGH, List.of(line()), Instant.now()
        );
    }

    private static OrderEventPayload.Line line() {
        return new OrderEventPayload.Line(UUID.randomUUID(), 1, new BigDecimal("99.90"));
    }
}
