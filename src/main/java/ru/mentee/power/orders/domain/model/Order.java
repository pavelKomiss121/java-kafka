package ru.mentee.power.orders.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Order {
    private UUID id;
    private UUID customerId;
    private String region;
    private OrderPriority priority;
    private BigDecimal amount;
    private OrderStatus status;
    private Instant createdAt;
    private List<OrderLine> lines = new ArrayList<>();

    public static Order createNew(UUID customerId, String region, OrderPriority priority,
                                  BigDecimal amount, List<OrderLine> lines) {
        Order order = new Order();
        order.id = UUID.randomUUID();
        order.customerId = customerId;
        order.region = region;
        order.priority = priority;
        order.amount = amount;
        order.lines = lines;
        order.status = OrderStatus.NEW; // или отдельный QUEUED на уровне ответа API
        order.createdAt = Instant.now();
        return order;
    }

    public UUID getId() {
        return id;
    }
    public UUID getCustomerId() {
        return customerId;
    }
    public String getRegion() {
        return region;
    }
    public OrderPriority getPriority() {
        return priority;
    }
    public BigDecimal getAmount() {
        return amount;
    }
    public OrderStatus getStatus() {
        return status;
    }
    public Instant getCreatedAt() {
        return createdAt;
    }
    public List<OrderLine> getLines() {
        return lines;
    }
}