package ru.mentee.power.orders.adapters.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import ru.mentee.power.orders.adapters.integration.OutOfStockException;
import ru.mentee.power.orders.adapters.persistence.OrderEntity;
import ru.mentee.power.orders.adapters.persistence.OrderRepository;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.domain.model.OrderStatus;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.PaymentsClient;
import ru.mentee.power.orders.ports.outgoing.PricingClient;
import ru.mentee.power.orders.ports.outgoing.WarehouseClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * С MKAFKA-08 добавлены @MockBean WarehouseClient/PaymentsClient — реальные
 * WarehouseHttpClient/PaymentsHttpClient в itest не участвуют (см. §10.2:
 * base-url в application-itest.yml нужен только для конструирования бинов).
 */
@SpringBootTest
@ActiveProfiles("itest")
@EmbeddedKafka(partitions = 1, topics = {
        "orders.priority.high", "orders.priority.normal", "orders.priority.low"
})
class OrderEventListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, OrderEventPayload> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @MockBean
    private PricingClient pricingClient;

    @MockBean
    private WarehouseClient warehouseClient;

    @MockBean
    private PaymentsClient paymentsClient;

    @Test
    void listenerMarksOrderProcessed_whenSagaSucceeds() throws InterruptedException {
        when(pricingClient.fetchDiscount(any(), anyString())).thenReturn(BigDecimal.ZERO);
        when(warehouseClient.reserve(any(), any(), anyInt())).thenReturn(UUID.randomUUID());
        when(paymentsClient.charge(any(), any())).thenReturn(UUID.randomUUID());

        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId, "EU", new BigDecimal("99.90"));

        savePendingOrder(orderId, payload);
        kafkaTemplate.send("orders.priority.high", "EU", payload);

        awaitTerminal(orderId);

        assertTrue(orderRepository.findById(orderId).map(OrderEntity::getProcessedAt).isPresent());
        assertEquals(OrderStatus.NEW.name(), orderRepository.findById(orderId).orElseThrow().getStatus());
    }

    @Test
    void listenerCancelsOrder_andReleasesStock_whenPaymentDeclined() throws InterruptedException {
        UUID orderId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();
        when(warehouseClient.reserve(any(), any(), anyInt())).thenReturn(reservationId);
        when(pricingClient.fetchDiscount(any(), anyString())).thenReturn(BigDecimal.ZERO);
        when(paymentsClient.charge(any(), any()))
                .thenThrow(new ru.mentee.power.orders.adapters.integration.PaymentDeclinedException(orderId));

        OrderEventPayload payload = payload(orderId, "EU", new BigDecimal("50.00"));
        savePendingOrder(orderId, payload);
        kafkaTemplate.send("orders.priority.normal", "EU", payload);

        awaitTerminal(orderId);

        assertEquals(OrderStatus.CANCELLED.name(), orderRepository.findById(orderId).orElseThrow().getStatus());
        verify(warehouseClient).release(reservationId);
    }

    @Test
    void listenerCancelsOrder_withoutCallingPayments_whenOutOfStock() throws InterruptedException {
        UUID orderId = UUID.randomUUID();
        when(warehouseClient.reserve(any(), any(), anyInt()))
                .thenThrow(new OutOfStockException(orderId, UUID.randomUUID()));

        OrderEventPayload payload = payload(orderId, "EU", new BigDecimal("30.00"));
        savePendingOrder(orderId, payload);
        kafkaTemplate.send("orders.priority.low", "EU", payload);

        awaitTerminal(orderId);

        assertEquals(OrderStatus.CANCELLED.name(), orderRepository.findById(orderId).orElseThrow().getStatus());
        verifyNoInteractions(paymentsClient);
    }

    @Test
    void listenerSkipsRedeliveredEvent_withSameEventId() throws InterruptedException {
        when(pricingClient.fetchDiscount(any(), anyString())).thenReturn(BigDecimal.ZERO);
        when(warehouseClient.reserve(any(), any(), anyInt())).thenReturn(UUID.randomUUID());
        when(paymentsClient.charge(any(), any())).thenReturn(UUID.randomUUID());

        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = payload(orderId, "EU", new BigDecimal("50.00"));
        savePendingOrder(orderId, payload);

        kafkaTemplate.send("orders.priority.normal", "EU", payload);
        kafkaTemplate.send("orders.priority.normal", "EU", payload);

        awaitTerminal(orderId);
        Thread.sleep(500);

        verify(paymentsClient, times(1)).charge(any(), any());
    }

    private OrderEventPayload payload(UUID orderId, String region, BigDecimal amount) {
        return new OrderEventPayload(
                UUID.randomUUID(), orderId, UUID.randomUUID(), region, amount,
                OrderPriority.HIGH,
                List.of(new OrderEventPayload.Line(UUID.randomUUID(), 1, amount)),
                Instant.now()
        );
    }

    private void savePendingOrder(UUID orderId, OrderEventPayload payload) {
        OrderEntity pending = new OrderEntity();
        pending.setId(orderId);
        pending.setCustomerId(payload.customerId());
        pending.setRegion(payload.region());
        pending.setPriority(payload.priority().name());
        pending.setAmount(payload.amount());
        pending.setStatus(OrderStatus.NEW.name());
        pending.setCreatedAt(Instant.now());
        orderRepository.save(pending);
    }

    private void awaitTerminal(UUID orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && orderRepository.findById(orderId).map(OrderEntity::getProcessedAt).isEmpty()) {
            Thread.sleep(200);
        }
    }
}