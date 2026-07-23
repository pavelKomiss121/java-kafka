package ru.mentee.power.orders.domain.model;

import java.math.BigDecimal;
import java.util.UUID;

public class OrderLine {
    private UUID productId;
    private int quantity;
    private BigDecimal price;
    private Priority priority;

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }


    public enum  Priority {
        HIGH,
        NORMAL,
        LOW
    }
}
