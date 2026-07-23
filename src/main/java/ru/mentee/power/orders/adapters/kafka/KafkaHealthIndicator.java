package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "app.kafka.health-enabled", havingValue = "true", matchIfMissing = true)
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;
    private final String topic;

    public KafkaHealthIndicator(
            KafkaAdmin kafkaAdmin,
            @Value("${app.kafka.topic}") String topic
    ) {
        this.kafkaAdmin = kafkaAdmin;
        this.topic = topic;
    }

    @Override
    public Health health() {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // 1) брокер отвечает
            client.describeCluster().nodes().get(3, TimeUnit.SECONDS);

            // 2) топик существует
            DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
            result.allTopicNames().get(3, TimeUnit.SECONDS);

            return Health.up()
                    .withDetail("topic", topic)
                    .withDetail("broker", "reachable")
                    .build();
        } catch (Exception e) {
            return Health.down(e)
                    .withDetail("topic", topic)
                    .build();
        }
    }
}
