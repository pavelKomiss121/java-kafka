package ru.mentee.power.orders.domain.usecase;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;
import ru.mentee.power.orders.ports.outgoing.OutboxStorePort;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Use case = сценарий «оформить заказ». С MKAFKA-06 не знает про Kafka вообще —
 * только сохраняет заказ и намерение опубликовать событие, одной транзакцией.
 * Публикацией занимается отдельный OutboxDispatchUseCase, вызываемый планировщиком.
 */
@Service
public class PlaceOrderUseCase implements PlaceOrderPort {

    private final OrderPersistencePort persistencePort;
    private final OutboxStorePort outboxStorePort;

    public PlaceOrderUseCase(OrderPersistencePort persistencePort, OutboxStorePort outboxStorePort) {
        this.persistencePort = persistencePort;
        this.outboxStorePort = outboxStorePort;
    }

    @Override
    @Transactional
    public PlaceOrderResult place(PlaceOrderCommand command) {
        validate(command);

        Order order = Order.createNew(
                command.customerId(),
                command.region(),
                command.priority(),
                command.amount(),
                command.lines()
        );

        persistencePort.savePending(order);

        OrderEventPayload payload = OrderEventPayload.from(order);
        outboxStorePort.append(order.getId(), payload);

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