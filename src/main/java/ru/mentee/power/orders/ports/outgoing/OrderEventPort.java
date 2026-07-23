package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;

public interface OrderEventPort {
    void publishOrderCreated(Order order);
}
