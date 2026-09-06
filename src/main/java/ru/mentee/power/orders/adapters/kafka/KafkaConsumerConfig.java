package ru.mentee.power.orders.adapters.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;

/**
 * Фабрика listener-контейнеров с ручным ack (MANUAL_IMMEDIATE) и ограниченным retry
 * на уровне контейнера. ConsumerFactory берём автосконфигурированный Boot'ом
 * из spring.kafka.consumer.* (application-local.yml) — как и KafkaTemplate в продюсере,
 * не пересобираем его руками.
 */
@Configuration
@Profile("!ci")
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEventPayload> orderConsumerContainerFactory(
            ConsumerFactory<String, OrderEventPayload> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEventPayload> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // 3 попытки с паузой 2с — временная мера до DLQ в MKAFKA-05
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(2_000L, 3)));
        return factory;
    }
}