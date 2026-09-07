package ru.mentee.power.orders.adapters.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestClient;

@Configuration
@Profile("!ci")
public class PricingClientConfig {

    @Bean
    public RestClient pricingRestClient(@Value("${pricing.base-url}") String baseUrl) {
        return RestClient.builder().baseUrl(baseUrl).build();
    }
}