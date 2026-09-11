package ru.mentee.power.orders.ports.outgoing;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Исходящий порт: «домену нужно узнать скидку для заказа перед сохранением».
 * Реализация (PricingHttpClient) знает про HTTP и Resilience4j — домен не знает ни про то, ни про другое.
 */
public interface PricingClient {

    BigDecimal fetchDiscount(UUID orderId, String region);
}