package ru.mentee.power.orders.ports.outgoing;

import java.math.BigDecimal;
import java.util.UUID;

public interface PaymentsClient {

    UUID charge(UUID orderId, BigDecimal amount);

    void refund(UUID paymentId);
}