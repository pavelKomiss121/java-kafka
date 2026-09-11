package ru.mentee.power.orders.domain.saga.steps;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.saga.OrderSagaContext;
import ru.mentee.power.orders.domain.saga.SagaStep;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderPersistencePort;

import java.math.BigDecimal;
import java.util.List;

/**
 * Последний, самый необратимый шаг (теория §2.2) — компенсировать здесь
 * НЕЧЕГО ВПЕРЁД: если этот шаг падает, orchestrator откатывает ПРЕДЫДУЩИЕ
 * шаги (ChargePayment, ReserveStock), а не этот. compensate() пуст осознанно.
 */
@Component
public class ConfirmOrderStep implements SagaStep<OrderSagaContext> {

    private final OrderPersistencePort persistencePort;

    public ConfirmOrderStep(OrderPersistencePort persistencePort) {
        this.persistencePort = persistencePort;
    }

    @Override
    public void execute(OrderSagaContext context) {
        Order order = toDomain(context);
        persistencePort.markProcessed(order, context.partition(), context.offset());
    }

    @Override
    public void compensate(OrderSagaContext context) {
        // Намеренно пусто: ConfirmOrder — последний шаг цепочки, он либо
        // выполняется целиком, либо не начинается вовсе. Откат предыдущих
        // шагов при его провале выполняет orchestrator, а не этот метод.
    }

    @Override
    public String name() {
        return "ConfirmOrder";
    }

    private Order toDomain(OrderSagaContext context) {
        OrderEventPayload payload = context.payload();
        List<OrderLine> lines = payload.lines().stream()
                .map(line -> {
                    OrderLine orderLine = new OrderLine();
                    orderLine.setProductId(line.productId());
                    orderLine.setQuantity(line.quantity());
                    orderLine.setPrice(line.price());
                    return orderLine;
                })
                .toList();

        BigDecimal amountAfterDiscount = payload.amount()
                .multiply(BigDecimal.ONE.subtract(context.discount()));

        return Order.restoreFromEvent(
                payload.orderId(),
                payload.customerId(),
                payload.region(),
                payload.priority(),
                amountAfterDiscount,
                lines
        );
    }
}