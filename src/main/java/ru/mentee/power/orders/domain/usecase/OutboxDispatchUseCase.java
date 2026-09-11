package ru.mentee.power.orders.domain.usecase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.mentee.power.orders.adapters.metrics.OutboxMetricsRegistry;
import ru.mentee.power.orders.ports.incoming.DispatchOutboxPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;
import ru.mentee.power.orders.ports.outgoing.OutboxEvent;
import ru.mentee.power.orders.ports.outgoing.OutboxStorePort;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Use case = сценарий «опубликовать накопившиеся outbox-события».
 * Публикацию делает тот же OrderEventPort/OrderEventProducer, что и в MKAFKA-03 —
 * этот класс не знает ни про Kafka, ни про имена топиков, только про порт.
 */

@Service
public class OutboxDispatchUseCase implements DispatchOutboxPort {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatchUseCase.class);

    private final OutboxStorePort outboxStorePort;
    private final OrderEventPort orderEventPort;
    private final OutboxMetricsRegistry metrics;
    private final int maxAttempts;

    public OutboxDispatchUseCase(OutboxStorePort outboxStorePort, OrderEventPort orderEventPort, OutboxMetricsRegistry metrics, @Value("${app.outbox.max-attempts:5}") int maxAttempts) {
        this.outboxStorePort = outboxStorePort;
        this.orderEventPort = orderEventPort;
        this.metrics = metrics;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public DispatchResult dispatchDueBatch(int batchSize) {
        List<OutboxEvent> batch = outboxStorePort.fetchDueBatch(batchSize);
        int sent = 0;
        int failed = 0;
        int dead = 0;

        for (OutboxEvent event : batch) {
            long startedAtNanos = System.nanoTime();
            try {
                orderEventPort.publish(event.payload(), event.payload().priority())
                        .toCompletableFuture()
                        .join();
                outboxStorePort.markSent(event.id());
                metrics.recordSent(Duration.ofNanos(System.nanoTime() - startedAtNanos));
                sent++;
            } catch (Exception ex) {
                String error = rootMessage(ex);
                if (event.attempts() + 1 >= maxAttempts) {
                    outboxStorePort.markDead(event.id(), error);
                    metrics.recordDead();
                    dead++;
                    log.warn("Outbox event moved to DEAD after {} attempts: orderId={}, error={}",
                            event.attempts() + 1, event.orderId(), error);
                } else {
                    Instant nextRetryAt = Instant.now().plus(backoff(event.attempts()));
                    outboxStorePort.markFailed(event.id(), error, nextRetryAt);
                    metrics.recordFailed();
                    failed++;
                    log.warn("Outbox event publish failed, will retry at {}: orderId={}, attempt={}, error={}",
                            nextRetryAt, event.orderId(), event.attempts() + 1, error);
                }
            }
        }
        return new DispatchResult(sent, failed, dead);
    }

    private Duration backoff(int attempts) {
        long seconds = Math.min(60, (long) Math.pow(2, attempts));
        return Duration.ofSeconds(seconds);
    }

    private String rootMessage(Throwable ex) {
        Throwable cause = ex;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    }
}
