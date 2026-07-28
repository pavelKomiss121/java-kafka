package ru.mentee.power.orders.adapters.kafka;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.metrics.ProducerMetricsRegistry;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Заглушка для профиля ci: тесты без живого брокера.
 */
@Component
@Profile("ci")
public class CiOrderEventAdapter implements OrderEventPort {

    private final ProducerMetricsRegistry metrics;

    public CiOrderEventAdapter(ProducerMetricsRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public CompletionStage<Void> publish(OrderEventPayload payload, OrderPriority priority) {
        String topic = "ci." + priority.name().toLowerCase();
        metrics.success(topic);
        return CompletableFuture.completedFuture(null);
    }
}
