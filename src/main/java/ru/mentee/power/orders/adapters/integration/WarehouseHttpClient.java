package ru.mentee.power.orders.adapters.integration;

import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import ru.mentee.power.orders.ports.outgoing.WarehouseClient;

import java.util.UUID;

/**
 * HTTP-адаптер к складу. 409 (OUT_OF_STOCK) — деловой ответ, не ретраится и не
 * оборачивается в ExternalServiceException: это осознанное решение сервиса,
 * а не сбой инфраструктуры (теория §2.7). Любая другая ошибка (5xx, таймаут,
 * сеть) — ExternalServiceException, единственный тип, который повторяет
 * Resilience4j (см. retry-exceptions в application.yml).
 */
@Component
@Profile("!ci")
public class WarehouseHttpClient implements WarehouseClient {

    private final RestClient restClient;

    public WarehouseHttpClient(@Qualifier("warehouseRestClient") RestClient restClient) {
        this.restClient = restClient;
    }

    @Override
    @Retry(name = "warehouse-processing", fallbackMethod = "fallbackReserve")
    public UUID reserve(UUID orderId, UUID productId, int quantity) {
        try{
            ReservationResponse response = restClient.post()
                    .uri("/internal/warehouse/reserve")
                    .body(new ReservationRequest(orderId, productId, quantity))
                    .retrieve()
                    .body(ReservationResponse.class);
            return response.reservationId();
        } catch (HttpClientErrorException.Conflict ex){
            throw new OutOfStockException(orderId, productId);
        } catch (RestClientException ex) {
            throw new ExternalServiceException(
                    "Warehouse reserve call failed for order " + orderId, ex);
        }
    }

    @Override
    public void release(UUID reservationId) {
        try{
            restClient.post()
                    .uri("/internal/warehouse/release")
                    .body(new ReleaseRequest(reservationId))
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ex) {
            throw new ExternalServiceException(
                    "Warehouse release call failed for reservation " + reservationId, ex);
        }
    }

    @SuppressWarnings("unused")
    private UUID fallbackReserve(UUID orderId, UUID productId, int quantity, Throwable throwable) {
        if (throwable instanceof OutOfStockException oos) {
            throw oos;
        }
        throw new WarehouseUnavailableException(orderId, throwable);
    }

    private record ReservationRequest(UUID orderId, UUID productId, int quantity) {}
    private record ReservationResponse(UUID reservationId) {}
    private record ReleaseRequest(UUID reservationId) {}
}
