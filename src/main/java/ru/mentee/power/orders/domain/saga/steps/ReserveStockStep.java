package ru.mentee.power.orders.domain.saga.steps;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.saga.OrderSagaContext;
import ru.mentee.power.orders.domain.saga.SagaStep;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.WarehouseClient;

@Component
public class ReserveStockStep implements SagaStep<OrderSagaContext> {

    private final WarehouseClient warehouseClient;

    public ReserveStockStep(WarehouseClient warehouseClient) {
        this.warehouseClient = warehouseClient;
    }

    @Override
    public void execute(OrderSagaContext context) {
        // Курс упрощает домен до одной строки заказа на резерв — как и остальной
        // проект (см. OrderConsumerUseCase.toDomain), полноценный мульти-строчный
        // резерв — за рамками этого урока.
        OrderEventPayload.Line line = context.payload().lines().get(0);
        var reservationId = warehouseClient.reserve(context.orderId(), line.productId(), line.quantity());
        context.setReservationId(reservationId);
    }

    @Override
    public void compensate(OrderSagaContext context) {
        if (context.reservationId() != null) {
            warehouseClient.release(context.reservationId());
        }
    }

    @Override
    public String name() {
        return "ReserveStock";
    }
}
