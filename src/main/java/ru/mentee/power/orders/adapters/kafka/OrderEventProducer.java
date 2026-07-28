package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.metrics.ProducerMetricsRegistry;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Kafka-адаптер: реализует исходящий порт через KafkaTemplate.
 * Ключ сообщения = region, топик = f(priority).
 */
@Component
@Profile("!ci")
public class OrderEventProducer implements OrderEventPort {

    private final KafkaTemplate<String, OrderEventPayload> kafkaTemplate;
    private final KafkaTopicResolver topicResolver;
    private final ProducerMetricsRegistry metrics;

    public OrderEventProducer(
            KafkaTemplate<String, OrderEventPayload> kafkaTemplate,
            KafkaTopicResolver topicResolver,
            ProducerMetricsRegistry metrics
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicResolver = topicResolver;
        this.metrics = metrics;
    }

    @Override
    public CompletionStage<Void> publish(OrderEventPayload payload, OrderPriority priority) {
        String topic = topicResolver.resolve(priority);
        String key = payload.region();

        ProducerRecord<String, OrderEventPayload> record =
                new ProducerRecord<>(topic, key, payload);

        CompletableFuture<Void> result = new CompletableFuture<>();

        kafkaTemplate.send(record).whenComplete((sendResult, ex) -> {
            if (ex == null) {
                metrics.success(topic);
                result.complete(null);
            } else {
                metrics.failure(topic);
                result.completeExceptionally(ex);
            }
        });

        return result;
    }
}
