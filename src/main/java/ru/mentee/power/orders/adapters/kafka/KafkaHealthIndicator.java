package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "app.kafka.health-enabled", havingValue = "true", matchIfMissing = true)
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;
    private final String highTopic;
    private final String normalTopic;
    private final String lowTopic;

    public KafkaHealthIndicator(
            KafkaAdmin kafkaAdmin,
            @Value("${app.kafka.topics.high}") String highTopic,
            @Value("${app.kafka.topics.normal}") String normalTopic,
            @Value("${app.kafka.topics.low}") String lowTopic
    ) {
        this.kafkaAdmin = kafkaAdmin;
        this.highTopic = highTopic;
        this.normalTopic = normalTopic;
        this.lowTopic = lowTopic;
    }

    @Override
    public Health health() {
        List<String> topics = List.of(highTopic, normalTopic, lowTopic);
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            client.describeCluster().nodes().get(3, TimeUnit.SECONDS);
            DescribeTopicsResult result = client.describeTopics(topics);
            result.allTopicNames().get(3, TimeUnit.SECONDS);

            return Health.up()
                    .withDetail("topics", topics)
                    .withDetail("broker", "reachable")
                    .build();
        } catch (Exception e) {
            return Health.down(e)
                    .withDetail("topics", topics)
                    .build();
        }
    }
}
