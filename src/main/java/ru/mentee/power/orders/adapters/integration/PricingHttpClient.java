package ru.mentee.power.orders.adapters.integration;

import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * HTTP-адаптер к внешнему сервису расчёта скидки.
 * @Retry живёт здесь, а не в OrderConsumerUseCase — вызов идёт через порт (другой Spring-бин),
 * значит проходит через AOP-прокси. Self-invocation внутри одного класса retry-advice не запускает
 * (см. теорию §2.1).
 */
@Component
@Profile("!ci")
public class PricingHttpClient implements PricingClient {

    private final RestClient restClient;

    public PricingHttpClient(@Qualifier("pricingRestClient") RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    @Retry(name = "order-processing", fallbackMethod = "fallbackDiscount")
    public BigDecimal fetchDiscount(UUID orderId, String region) {
        try {
            PricingResponse response = restClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/internal/pricing")
                            .queryParam("orderId", orderId)
                            .queryParam("region", region)
                            .build())
                    .retrieve()
                    .body(PricingResponse.class);
            return response.discount();
        } catch (RestClientException ex) {
            throw new ExternalServiceException(
                    "Pricing service call failed for order " + orderId, ex);
        }
    }

    @SuppressWarnings("unused")
    private BigDecimal fallbackDiscount(UUID orderId, String region, Throwable throwable) {
        throw new PricingUnavailableException(orderId, throwable);
    }
}