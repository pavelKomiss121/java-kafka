package ru.mentee.power.orders.adapters.web;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.mentee.power.orders.adapters.integration.PricingResponse;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Учебная заглушка внешнего сервиса расчёта скидки. В проде это отдельный микросервис;
 * здесь для простоты pet-project'а он живёт в этом же приложении — важен паттерн
 * retry/fallback вокруг вызова, а не реальная межсервисная интеграция.
 */
@RestController
@RequestMapping("/internal/pricing")
@Profile("local")
public class PricingStubController {

    @GetMapping
    public PricingResponse getPricing(@RequestParam UUID orderId, @RequestParam String region) {
        BigDecimal discount = "EU".equals(region) ? new BigDecimal("0.05") : BigDecimal.ZERO;
        return new PricingResponse(discount);
    }
}