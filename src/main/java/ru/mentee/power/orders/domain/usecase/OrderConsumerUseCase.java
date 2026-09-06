package ru.mentee.power.orders.domain.usecase;

import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.ports.incoming.ProcessOrderEventPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;

import java.util.List;

@Service
public class OrderConsumerUseCase implements ProcessOrderEventPort {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumerUseCase.class);

    private final OrderPersistencePort persistencePort;
    private final ConsumerMetricsRegistry metrics;

    public OrderConsumerUseCase(OrderPersistencePort persistencePort, ConsumerMetricsRegistry metrics) {
        this.persistencePort = persistencePort;
        this.metrics = metrics;
    }


    @Override
    @Transactional
    public void handle(OrderEventPayload payload, int partition, long offset) {
        validate(payload);

        if (persistencePort.existsById(payload.orderId())){
            metrics.duplicate();
            log.info("Duplicate order event skipped: orderId={}, partition={}, offset={}",
                    payload.orderId(), partition, offset);
            return;
        }

        Order order = toDomain(payload);
        persistencePort.save(order,  partition, offset);
        metrics.processed(payload.priority(), payload.region());

    }

    private void validate(OrderEventPayload payload) {
        if (payload.orderId() ==  null)
            throw new IllegalArgumentException("Order ID is required");
        if (payload.priority() ==  null)
            throw new IllegalArgumentException("Priority is required");
        if (payload.region() == null || payload.region().isBlank())
            throw new IllegalArgumentException("Region is required");
    }

    private Order toDomain(OrderEventPayload payload) {
        List<OrderLine> lines = payload.lines().stream()
                .map(line -> {
                    OrderLine orderLine = new OrderLine();
                    orderLine.setProductId(line.productId());
                    orderLine.setQuantity(line.quantity());
                    orderLine.setPrice(line.price());
                    return orderLine;
                })
                .toList();

        return Order.restoreFromEvent(
                payload.orderId(),
                payload.customerId(),
                payload.region(),
                payload.priority(),
                payload.amount(),
                lines
        );
    }
}
