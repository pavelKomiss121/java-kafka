package ru.mentee.power.orders.adapters.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.OrderPriority;

@Component
@Profile("!ci")
public class KafkaTopicResolver {

    private final String high;
    private final String normal;
    private final String low;

    public KafkaTopicResolver(
            @Value("${app.kafka.topics.high}") String high,
            @Value("${app.kafka.topics.normal}") String normal,
            @Value("${app.kafka.topics.low}") String low
    ) {
        this.high = high;
        this.normal = normal;
        this.low = low;
    }

    public String resolve(OrderPriority priority) {
        return switch (priority) {
            case HIGH -> high;
            case NORMAL -> normal;
            case LOW -> low;
        };
    }
}
