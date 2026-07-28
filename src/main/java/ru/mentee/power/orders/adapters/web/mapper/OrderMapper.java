package ru.mentee.power.orders.adapters.web.mapper;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.web.dto.OrderAcceptedResponse;
import ru.mentee.power.orders.adapters.web.dto.OrderRequest;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;

import java.util.List;

/**
 * Перевод HTTP DTO → команда для домена (и обратно в ответ).
 * Domain не должен знать про JSON.
 */
@Component
public class OrderMapper {

    public PlaceOrderPort.PlaceOrderCommand toCommand(OrderRequest request) {
        if (request.getPriority() == null || request.getPriority().isBlank()) {
            throw new IllegalArgumentException("priority must not be blank");
        }
        OrderPriority priority;
        try {
            priority = OrderPriority.valueOf(request.getPriority());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("priority must be HIGH, NORMAL or LOW");
        }

        List<OrderLine> lines = request.getLines() == null
                ? List.of()
                : request.getLines().stream().map(this::toLine).toList();

        return new PlaceOrderPort.PlaceOrderCommand(
                request.getCustomerId(),
                request.getRegion(),
                priority,
                request.getAmount(),
                lines
        );
    }

    public OrderAcceptedResponse toAccepted(PlaceOrderPort.PlaceOrderResult result) {
        return new OrderAcceptedResponse(result.orderId(), result.status(), result.dispatchedAt());
    }

    private OrderLine toLine(OrderRequest.OrderLineDto dto) {
        OrderLine line = new OrderLine();
        line.setProductId(dto.getProductId());
        line.setQuantity(dto.getQuantity());
        line.setPrice(dto.getPrice());
        return line;
    }
}
