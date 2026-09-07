package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.ports.outgoing.DeadLetterPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * DLQ-адаптер: событие, для которого исчерпан retry, улетает в orders.priority.dlq
 * с тем же payload (см. теорию §2.5) и заголовками-метаданными об ошибке.
 */
@Component
@Profile("!ci")
public class DeadLetterPublisher implements DeadLetterPort {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final KafkaTemplate<String, OrderEventPayload> kafkaTemplate;
    private final KafkaTopicResolver topicResolver;
    private final String dlqTopic;

    public DeadLetterPublisher(
            KafkaTemplate<String, OrderEventPayload> kafkaTemplate,
            KafkaTopicResolver topicResolver,
            @Value("${app.kafka.topics.dlq}") String dlqTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicResolver = topicResolver;
        this.dlqTopic = dlqTopic;
    }

    @Override
    public void publish(OrderEventPayload payload, int partition, long offset, Throwable cause) {
        ProducerRecord<String, OrderEventPayload> record =
                new ProducerRecord<>(dlqTopic, payload.region(), payload);

        String errorCode = cause.getClass().getSimpleName();
        record.headers()
                .add(new RecordHeader("errorCode", errorCode.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("originalTopic",
                        topicResolver.resolve(payload.priority()).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("originalPartition",
                        String.valueOf(partition).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("originalOffset",
                        String.valueOf(offset).getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("failedAt",
                        Instant.now().toString().getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record);
        log.warn("Order routed to DLQ: orderId={}, topic={}, errorCode={}",
                payload.orderId(), dlqTopic, errorCode);
    }
}