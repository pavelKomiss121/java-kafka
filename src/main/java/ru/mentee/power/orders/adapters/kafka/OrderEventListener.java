package ru.mentee.power.orders.adapters.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.ports.incoming.ProcessOrderEventPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;

/**
 * Kafka-адаптер: читает orders.priority.* и подтверждает offset вручную,
 * только после успешного сохранения в БД (через ProcessOrderEventPort).
 */
@Component
@Profile("!ci")
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final ProcessOrderEventPort processOrderEventPort;
    private final ConsumerMetricsRegistry metrics;

    public OrderEventListener(ProcessOrderEventPort processOrderEventPort, ConsumerMetricsRegistry metrics) {
        this.processOrderEventPort = processOrderEventPort;
        this.metrics = metrics;
    }

    @KafkaListener(
            id = "order-consumer",
            topics = {
                    "${app.kafka.topics.high}",
                    "${app.kafka.topics.normal}",
                    "${app.kafka.topics.low}"
            },
            containerFactory = "orderConsumerContainerFactory")
    public void listen(
            @Payload OrderEventPayload payload,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment ack) {
        try {
            processOrderEventPort.handle(payload, partition, offset);
            acknowledge(ack);
        } catch (IllegalArgumentException ex) {
            // "ядовитое" сообщение: повторная попытка ничего не исправит — считаем метрику и коммитим офсет
            metrics.rejected();
            log.warn("Rejected invalid order event at partition={}, offset={}: {}",
                    partition, offset, ex.getMessage());
            acknowledge(ack);
        }
        // остальные исключения (например, недоступна БД) не ловим намеренно:
        // офсет не коммитится, DefaultErrorHandler повторит доставку (см. KafkaConsumerConfig)
    }

    private void acknowledge(Acknowledgment ack) {
        if (ack != null) {
            ack.acknowledge();
        }
    }
}