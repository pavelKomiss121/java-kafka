package ru.mentee.power.orders.adapters.web.mapper;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.web.dto.OrderRequest;
import ru.mentee.power.orders.adapters.web.dto.OrderResponse;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;

import java.util.stream.Collectors;

@Component
public class OrderMapper {

    public Order toDomain(OrderRequest request) {
        Order order = new Order();
        order.setCustomerId(request.getCustomerId());
        order.setAmount(request.getAmount());
        if(request.getLines()!=null) {
            order.setLines(request.getLines().stream().map(this::toLine).collect(Collectors.toList()));
        }
        return order;
    }

    public OrderResponse toResponse(Order order) {
        OrderResponse response = new OrderResponse();
        response.setOrderId(order.getId());
        response.setStatus(order.getStatus() != null ? order.getStatus().name() : null);
        response.setAmount(order.getAmount());
        response.setCreatedAt(order.getCreatedAt());
        return response;
    }

    private OrderLine toLine(OrderRequest.OrderLineDto dto) {
        OrderLine line = new OrderLine();
        line.setProductId(dto.getProductId());
        line.setQuantity(dto.getQuantity());
        line.setPrice(dto.getPrice());
        if (dto.getPriority() != null) {
            line.setPriority(OrderLine.Priority.valueOf(dto.getPriority()));
        }
        return line;
    }
}
