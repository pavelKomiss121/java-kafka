package ru.mentee.power.orders.adapters.kafka;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

@Component
public class NoOpOrderEventAdapter implements OrderEventPort {
    @Override
    public void publishOrderCreated(Order order) {
        // TODO MKAFKA-03: KafkaTemplate.send(topic, orderId, payload)
    }
}
