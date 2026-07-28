package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("!ci")
public class KafkaProducerConfig {

    @Bean
    NewTopic ordersPriorityHigh(@Value("${app.kafka.topics.high}") String name){
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic ordersPriorityNormal(@Value("${app.kafka.topics.normal}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    NewTopic ordersPriorityLow(@Value("${app.kafka.topics.low}") String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
