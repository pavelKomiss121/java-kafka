package ru.mentee.power.orders.adapters.persistence;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * JPA-адаптер: реализует исходящий порт. С MKAFKA-06 разделён на две операции —
 * savePending пишет producer-сторона (до Kafka), markProcessed — consumer-сторона
 * (после успешной обработки события). Раньше был единственный save(order, partition, offset),
 * вызываемый только консьюмером.
 */
@Component
public class OrderPersistenceAdapter implements OrderPersistencePort {

    private final OrderRepository orderRepository;
    private final OrderLineRepository orderLineRepository;

    public OrderPersistenceAdapter(OrderRepository orderRepository, OrderLineRepository orderLineRepository) {
        this.orderRepository = orderRepository;
        this.orderLineRepository = orderLineRepository;
    }

    @Override
    public boolean isAlreadyProcessed(UUID orderId) {
        return orderRepository.existsByIdAndProcessedAtIsNotNull(orderId);
    }

    @Override
    @Transactional
    public void savePending(Order order) {
        OrderEntity entity = new OrderEntity();
        entity.setId(order.getId());
        entity.setCustomerId(order.getCustomerId());
        entity.setRegion(order.getRegion());
        entity.setPriority(order.getPriority().name());
        entity.setAmount(order.getAmount());
        entity.setStatus(order.getStatus().name());
        entity.setCreatedAt(order.getCreatedAt());
        orderRepository.save(entity);

        List<OrderLineEntity> lines = order.getLines().stream()
                .map(line -> new OrderLineEntity(order.getId(), line.getProductId(), line.getQuantity(), line.getPrice()))
                .toList();
        orderLineRepository.saveAll(lines);
    }

    @Override
    @Transactional
    public void markProcessed(Order order, int partition, long offset) {
        OrderEntity entity = orderRepository.findById(order.getId())
                .orElseThrow(() -> new IllegalStateException(
                        "Order " + order.getId() + " must already exist (created by savePending via Outbox)"));
        entity.setAmount(order.getAmount());
        entity.setKafkaPartition(partition);
        entity.setKafkaOffset(offset);
        entity.setProcessedAt(Instant.now());
        orderRepository.save(entity);
    }
}