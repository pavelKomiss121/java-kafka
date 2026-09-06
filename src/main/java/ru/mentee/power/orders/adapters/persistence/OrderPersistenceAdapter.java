package ru.mentee.power.orders.adapters.persistence;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * JPA-адаптер: реализует исходящий порт сохранения через Spring Data.
 * Домен (OrderConsumerUseCase) ничего не знает про OrderEntity/JpaRepository.
 */
@Component
public class OrderPersistenceAdapter  implements OrderPersistencePort {

    private final OrderRepository orderRepository;
    private final OrderLineRepository orderLineRepository;

    public OrderPersistenceAdapter(OrderRepository orderRepository, OrderLineRepository orderLineRepository) {
        this.orderRepository = orderRepository;
        this.orderLineRepository = orderLineRepository;
    }

    @Override
    public boolean existsById(UUID orderId) {
        return orderRepository.existsById(orderId);
    }

    @Override
    @Transactional
    public void save(Order order, int partition, long offset) {
        OrderEntity entity = new OrderEntity();
        entity.setId(order.getId());
        entity.setCustomerId(order.getCustomerId());
        entity.setRegion(order.getRegion());
        entity.setPriority(order.getPriority().name());
        entity.setAmount(order.getAmount());
        entity.setStatus(order.getStatus().name());
        entity.setCreatedAt(order.getCreatedAt());
        entity.setKafkaPartition(partition);
        entity.setKafkaOffset(offset);
        entity.setProcessedAt(Instant.now());
        orderRepository.save(entity);

        List<OrderLineEntity> lines = order.getLines().stream()
                .map(line -> new OrderLineEntity(order.getId(), line.getProductId(), line.getQuantity(), line.getPrice()))
                .toList();
        orderLineRepository.saveAll(lines);

    }
}
