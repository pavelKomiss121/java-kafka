package ru.mentee.power.orders.adapters.integration;

import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import ru.mentee.power.orders.ports.outgoing.PaymentsClient;

import java.math.BigDecimal;
import java.util.UUID;

@Component
@Profile("!ci")
public class PaymentsHttpClient implements PaymentsClient {

    private final RestClient restClient;

    public PaymentsHttpClient(@Qualifier("paymentsRestClient") RestClient paymentsRestClient) {
        this.restClient = paymentsRestClient;
    }

    @Override
    @Retry(name = "payments-processing", fallbackMethod = "fallbackCharge")
    public UUID charge(UUID orderId, BigDecimal amount) {
        try {
            PaymentResponse response = restClient.post()
                    .uri("/internal/payments/charge")
                    .body(new ChargeRequest(orderId, amount))
                    .retrieve()
                    .body(PaymentResponse.class);
            return response.paymentId();
        } catch (HttpClientErrorException ex) {
            if (ex.getStatusCode().value() == 402) {
                throw new PaymentDeclinedException(orderId);
            }
            throw new ExternalServiceException(
                    "Payments charge call failed for order " + orderId, ex);
        } catch (RestClientException ex) {
            throw new ExternalServiceException(
                    "Payments charge call failed for order " + orderId, ex);
        }
    }

    @Override
    public void refund(UUID paymentId) {
        try {
            restClient.post()
                    .uri("/internal/payments/refund")
                    .body(new RefundRequest(paymentId))
                    .retrieve()
                    .toBodilessEntity();
        } catch (RestClientException ex) {
            throw new ExternalServiceException(
                    "Payments refund call failed for payment " + paymentId, ex);
        }
    }

    @SuppressWarnings("unused")
    private UUID fallbackCharge(UUID orderId, BigDecimal amount, Throwable throwable) {
        if (throwable instanceof PaymentDeclinedException declined) {
            throw declined;
        }
        throw new PaymentsUnavailableException(orderId, throwable);
    }

    private record ChargeRequest(UUID orderId, BigDecimal amount) {}
    private record PaymentResponse(UUID paymentId) {}
    private record RefundRequest(UUID paymentId) {}
}