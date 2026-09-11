package ru.mentee.power.orders.adapters.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import ru.mentee.power.orders.adapters.persistence.OrderEntity;
import ru.mentee.power.orders.adapters.persistence.OrderRepository;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.domain.model.OrderStatus;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Проверяет только Kafka-пайплайн (listener -> use case -> persistence).
 * С MKAFKA-06 строка в orders обязана существовать ДО прихода сообщения — тест
 * готовит её вручную перед отправкой.
 * С MKAFKA-07 добавлен второй тест: повторная доставка с ТЕМ ЖЕ eventId (как при
 * ретрае outbox-планировщика или редоставке Kafka) не должна доходить до
 * pricingClient второй раз — её обязан остановить dedup guard.
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

    @Test
    void listenerMarksOrderProcessed_whenPendingRowAlreadyExists() throws InterruptedException {
        when(pricingClient.fetchDiscount(any(), anyString())).thenReturn(BigDecimal.ZERO);

        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = new OrderEventPayload(
                UUID.randomUUID(),
                orderId,
                UUID.randomUUID(),
                "EU",
                new BigDecimal("99.90"),
                OrderPriority.HIGH,
                List.of(new OrderEventPayload.Line(UUID.randomUUID(), 1, new BigDecimal("99.90"))),
                Instant.now()
        );

        savePendingOrder(orderId, payload);

        kafkaTemplate.send("orders.priority.high", "EU", payload);

        awaitProcessed(orderId);

        assertTrue(orderRepository.findById(orderId).map(OrderEntity::getProcessedAt).isPresent(),
                "Order should be marked processed by OrderEventListener within timeout");
    }

    @Test
    void listenerSkipsRedeliveredEvent_withSameEventId() throws InterruptedException {
        when(pricingClient.fetchDiscount(any(), anyString())).thenReturn(BigDecimal.ZERO);

        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = new OrderEventPayload(
                UUID.randomUUID(),
                orderId,
                UUID.randomUUID(),
                "EU",
                new BigDecimal("50.00"),
                OrderPriority.NORMAL,
                List.of(new OrderEventPayload.Line(UUID.randomUUID(), 1, new BigDecimal("50.00"))),
                Instant.now()
        );

        savePendingOrder(orderId, payload);

        // Имитируем редоставку: ОДИН И ТОТ ЖЕ payload (тот же eventId) уходит дважды —
        // ровно то, что делает outbox-планировщик при ретрае неподтверждённой публикации.
        kafkaTemplate.send("orders.priority.normal", "EU", payload);
        kafkaTemplate.send("orders.priority.normal", "EU", payload);

        awaitProcessed(orderId);
        Thread.sleep(500); // дать второй доставке время дойти до listener'а, если guard не сработает

        verify(pricingClient, times(1)).fetchDiscount(orderId, "EU");
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

    private void awaitProcessed(UUID orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && orderRepository.findById(orderId).map(OrderEntity::getProcessedAt).isEmpty()) {
            Thread.sleep(200);
        }
    }
}