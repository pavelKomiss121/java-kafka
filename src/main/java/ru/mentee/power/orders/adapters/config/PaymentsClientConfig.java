package ru.mentee.power.orders.adapters.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestClient;

@Configuration
@Profile("!ci")
public class PaymentsClientConfig {

    @Bean
    @Qualifier("paymentsRestClient")
    public RestClient paymentsRestClient(@Value("${payments.base-url}") String baseUrl) {
        return RestClient.builder().baseUrl(baseUrl).build();
    }
}