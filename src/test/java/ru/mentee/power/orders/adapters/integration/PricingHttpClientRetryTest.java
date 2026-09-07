package ru.mentee.power.orders.adapters.integration;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestClient;
import ru.mentee.power.orders.ports.outgoing.PricingClient;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(classes = PricingHttpClientRetryTest.TestConfig.class)
@TestPropertySource(properties = "pricing.base-url=http://localhost:8089")
class PricingHttpClientRetryTest {

    private static final WireMockServer wireMockServer = new WireMockServer(8089);

    @Autowired
    private PricingClient pricingClient;

    @BeforeAll
    static void startWireMock() {
        wireMockServer.start();
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    @BeforeEach
    void resetStubs() {
        wireMockServer.resetAll();
    }

    @Test
    void retriesThreeTimesThenThrowsPricingUnavailable() {
        wireMockServer.stubFor(get(urlPathEqualTo("/internal/pricing"))
                .willReturn(aResponse().withStatus(500)));

        UUID orderId = UUID.randomUUID();

        assertThrows(PricingUnavailableException.class,
                () -> pricingClient.fetchDiscount(orderId, "EU"));

        wireMockServer.verify(3, getRequestedFor(urlPathEqualTo("/internal/pricing")));
    }

    @Test
    void succeedsWithoutRetryWhenServiceIsHealthy() {
        wireMockServer.stubFor(get(urlPathEqualTo("/internal/pricing"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"discount\": 0.05}")));

        UUID orderId = UUID.randomUUID();

        var discount = pricingClient.fetchDiscount(orderId, "EU");

        org.junit.jupiter.api.Assertions.assertEquals(0, discount.compareTo(new java.math.BigDecimal("0.05")));
        wireMockServer.verify(1, getRequestedFor(urlPathEqualTo("/internal/pricing")));
    }

    @Configuration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class,
            LiquibaseAutoConfiguration.class,
            KafkaAutoConfiguration.class
    })
    static class TestConfig {

        @Bean
        PricingClient pricingClient(@Value("${pricing.base-url}") String baseUrl) {
            return new PricingHttpClient(RestClient.builder().baseUrl(baseUrl).build());
        }
    }
}