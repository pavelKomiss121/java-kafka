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
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestClient;
import ru.mentee.power.orders.ports.outgoing.WarehouseClient;

import java.net.http.HttpClient;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(classes = WarehouseHttpClientRetryTest.TestConfig.class)
@TestPropertySource(properties = "warehouse.base-url=http://localhost:8092")
class WarehouseHttpClientRetryTest {

    private static final WireMockServer wireMockServer = new WireMockServer(8092);

    @Autowired
    private WarehouseClient warehouseClient;

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
    void retriesThreeTimesThenThrowsWarehouseUnavailable() {
        wireMockServer.stubFor(post(urlPathEqualTo("/internal/warehouse/reserve"))
                .willReturn(aResponse().withStatus(500)));

        assertThrows(WarehouseUnavailableException.class,
                () -> warehouseClient.reserve(UUID.randomUUID(), UUID.randomUUID(), 1));

        wireMockServer.verify(3, postRequestedFor(urlPathEqualTo("/internal/warehouse/reserve")));
    }

    @Test
    void doesNotRetry_onOutOfStockConflict() {
        wireMockServer.stubFor(post(urlPathEqualTo("/internal/warehouse/reserve"))
                .willReturn(aResponse().withStatus(409)));

        assertThrows(OutOfStockException.class,
                () -> warehouseClient.reserve(UUID.randomUUID(), UUID.randomUUID(), 1));

        wireMockServer.verify(1, postRequestedFor(urlPathEqualTo("/internal/warehouse/reserve")));
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
        WarehouseClient warehouseClient(@Value("${warehouse.base-url}") String baseUrl) {
            // Пин на HTTP/1.1: JDK HttpClient по умолчанию пытается h2c-апгрейд даже для
            // обычного http://, а WireMock (Jetty, только HTTP/1.1) отвечает на это
            // "Received RST_STREAM: Stream cancelled" — не связано с логикой ретраев/статус-кодов,
            // чисто транспортная несовместимость JDK 21 HttpClient + WireMock standalone.
            HttpClient jdkHttpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
            RestClient restClient = RestClient.builder()
                    .baseUrl(baseUrl)
                    .requestFactory(new JdkClientHttpRequestFactory(jdkHttpClient))
                    .build();
            return new WarehouseHttpClient(restClient);
        }
    }
}