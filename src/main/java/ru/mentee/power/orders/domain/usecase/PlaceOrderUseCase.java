package ru.mentee.power.orders.domain.usecase;

import org.springframework.stereotype.Service;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Use case = сценарий «оформить заказ».
 * Знает бизнес-правила и исходящий порт, но не знает HTTP и KafkaTemplate.
 */
@Service
public class PlaceOrderUseCase implements PlaceOrderPort {

    private final OrderEventPort orderEventPort;
    private final Map<UUID, Order> inMemoryStore = new ConcurrentHashMap<>();

    public PlaceOrderUseCase(OrderEventPort orderEventPort) {
        this.orderEventPort = orderEventPort;
    }

    @Override
    public PlaceOrderResult place(PlaceOrderCommand command) {
        validate(command);

        Order order = Order.createNew(
                command.customerId(),
                command.region(),
                command.priority(),
                command.amount(),
                command.lines()
        );

        inMemoryStore.put(order.getId(), order);
        // TODO MKAFKA-04: OrderRepository.save(...)

        OrderEventPayload payload = OrderEventPayload.from(order);

        orderEventPort.publish(payload, order.getPriority())
                .toCompletableFuture()
                .join();

        return new PlaceOrderResult(order.getId(), "QUEUED", Instant.now());
    }

    private void validate(PlaceOrderCommand command) {
        if (command.customerId() == null) {
            throw new IllegalArgumentException("customerId must not be null");
        }
        if (command.region() == null || command.region().isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
        if (command.priority() == null) {
            throw new IllegalArgumentException("priority must not be null");
        }
        if (command.amount() == null || command.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be greater than zero");
        }
        if (command.lines() == null || command.lines().isEmpty()) {
            throw new IllegalArgumentException("lines must not be empty");
        }
        for (OrderLine line : command.lines()) {
            if (line.getProductId() == null) {
                throw new IllegalArgumentException("productId must not be null");
            }
            if (line.getQuantity() < 1) {
                throw new IllegalArgumentException("quantity must be >= 1");
            }
            if (line.getPrice() == null || line.getPrice().compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalArgumentException("price must be >= 0");
            }
        }
    }
}
