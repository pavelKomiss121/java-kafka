package ru.mentee.power.orders.adapters.web.dto;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public class OrderRequest {
    private UUID customerId;
    private BigDecimal amount;
    private List<OrderLineDto> lines;

    public UUID getCustomerId() {
        return customerId;
    }
    public void setCustomerId(UUID customerId) {
        this.customerId = customerId;
    }
    public BigDecimal getAmount() {
        return amount;
    }
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
    public List<OrderLineDto> getLines() {
        return lines;
    }
    public void setLines(List<OrderLineDto> lines) {
        this.lines = lines;
    }

    public static class OrderLineDto {
        private UUID productId;
        private int quantity;
        private BigDecimal price;
        private String priority;


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
        public String getPriority() {
            return priority;
        }
        public void setPriority(String priority) {
            this.priority = priority;
        }
    }



}
