package ru.mentee.power.orders.ports.incoming;

import ru.mentee.power.orders.domain.model.Order;

public interface PlaceOrderPort {
    Order place(Order order);
}
