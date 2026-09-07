package ru.mentee.power.orders.adapters.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import ru.mentee.power.orders.adapters.persistence.OrderRepository;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Проверяет только Kafka-пайплайн (listener -> use case -> persistence).
 * Вызов внешнего сервиса скидок замокан — его retry/fallback/DLQ отдельно
 * покрыты PricingHttpClientRetryTest, без embedded Kafka.
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
    void listenerPersistsOrderFromKafkaEvent() throws InterruptedException {
        when(pricingClient.fetchDiscount(any(), anyString())).thenReturn(BigDecimal.ZERO);

        UUID orderId = UUID.randomUUID();
        OrderEventPayload payload = new OrderEventPayload(
                orderId,
                UUID.randomUUID(),
                "EU",
                new BigDecimal("99.90"),
                OrderPriority.HIGH,
                List.of(new OrderEventPayload.Line(UUID.randomUUID(), 1, new BigDecimal("99.90"))),
                Instant.now()
        );

        kafkaTemplate.send("orders.priority.high", "EU", payload);

        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline && orderRepository.findById(orderId).isEmpty()) {
            Thread.sleep(200);
        }

        assertTrue(orderRepository.findById(orderId).isPresent(),
                "Order should be persisted by OrderEventListener within timeout");
    }
}