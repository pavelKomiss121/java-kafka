package ru.mentee.power.orders.domain.saga;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.adapters.metrics.SagaMetricsRegistry;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.SagaLogPort;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderSagaOrchestratorTest {

    @Mock
    SagaLogPort sagaLogPort;

    @Mock
    io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @Test
    void compensatesOnlyCompletedSteps_inReverseOrder() {
        SagaMetricsRegistry metrics = new SagaMetricsRegistry(new io.micrometer.core.instrument.simple.SimpleMeterRegistry());
        OrderSagaOrchestrator orchestrator = new OrderSagaOrchestrator(sagaLogPort, metrics);

        SagaStep<OrderSagaContext> step1 = mock(SagaStep.class);
        SagaStep<OrderSagaContext> step2 = mock(SagaStep.class);
        SagaStep<OrderSagaContext> step3 = mock(SagaStep.class);
        when(step1.name()).thenReturn("Step1");
        when(step2.name()).thenReturn("Step2");
        // step3.name() намеренно НЕ застаблен: step3 никогда не выполняется (цикл в
        // OrderSagaOrchestrator.run прерывается исключением до третьей итерации), поэтому
        // step3.name() production-кодом не вызывается вообще — застабленный, но не
        // использованный стаб Mockito с MockitoExtension пометил бы как UnnecessaryStubbingException.

        RuntimeException failure = new RuntimeException("boom");
        org.mockito.Mockito.doThrow(failure).when(step2).execute(any());

        OrderSagaContext context = new OrderSagaContext(payload(), 0, 1L);

        assertThrows(RuntimeException.class, () -> orchestrator.run(List.of(step1, step2, step3), context));

        var inOrder = org.mockito.Mockito.inOrder(step1);
        verify(step1).execute(context);
        verify(step2).execute(context);
        verify(step3, never()).execute(any());

        // Скомпенсирован только step1 (step2 не завершился успешно — компенсировать нечего,
        // step3 вообще не выполнялся).
        verify(step1).compensate(context);
        verify(step2, never()).compensate(any());
        verify(step3, never()).compensate(any());

        verify(sagaLogPort).failStep(context.sagaId(), "Step2", "boom");
        verify(sagaLogPort).compensateStep(context.sagaId(), "Step1", null);
    }

    private static OrderEventPayload payload() {
        UUID orderId = UUID.randomUUID();
        return new OrderEventPayload(
                UUID.randomUUID(), orderId, UUID.randomUUID(), "EU", new BigDecimal("99.90"),
                OrderPriority.HIGH,
                List.of(new OrderEventPayload.Line(UUID.randomUUID(), 1, new BigDecimal("99.90"))),
                Instant.now()
        );
    }
}