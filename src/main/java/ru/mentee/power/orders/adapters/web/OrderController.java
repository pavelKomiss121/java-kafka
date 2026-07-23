package ru.mentee.power.orders.adapters.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.mentee.power.orders.adapters.web.dto.OrderRequest;
import ru.mentee.power.orders.adapters.web.dto.OrderResponse;
import ru.mentee.power.orders.adapters.web.mapper.OrderMapper;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {
    private final PlaceOrderPort placeOrderPort;
    private final OrderMapper orderMapper;

    public OrderController(PlaceOrderPort placeOrderPort, OrderMapper orderMapper) {
        this.placeOrderPort = placeOrderPort;
        this.orderMapper = orderMapper;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@RequestBody OrderRequest request)  {
        Order domain = orderMapper.toDomain(request);
        Order created = placeOrderPort.place(domain);

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(orderMapper.toResponse(created));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable("orderId") String orderId) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
    }

}
