package ru.mentee.power.orders.domain.usecase;

import org.springframework.stereotype.Service;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

@Service
public class PlaceOrderUseCase implements PlaceOrderPort {

    private final OrderEventPort orderEventPort;

    public PlaceOrderUseCase(OrderEventPort orderEventPort) {
        this.orderEventPort = orderEventPort;
    }

    @Override
    public Order place(Order order) {
        throw new UnsupportedOperationException("TODO MKAFKA-03: implement place order");
    }
}
