package ru.mentee.power.orders.adapters.metrics;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.persistence.outbox.OutboxEventRepository;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * В отличие от ProducerMetricsRegistry/ConsumerMetricsRegistry (свои счётчики + REST),
 * здесь источник правды — те же AtomicLong, но дополнительно связанные с Micrometer
 * (FunctionCounter/Gauge) — так /actuator/prometheus и GET /outbox/metrics
 * показывают одни и те же числа, без дублирования состояния.
 */
@Component
public class OutboxMetricsRegistry {

    private final AtomicLong sentTotal = new AtomicLong();
    private final AtomicLong failedTotal = new AtomicLong();
    private final AtomicLong deadTotal = new AtomicLong();
    private final Timer publishTimer;
    private final OutboxEventRepository repository;

    public OutboxMetricsRegistry(MeterRegistry registry, OutboxEventRepository repository) {
        this.repository = repository;
        this.publishTimer = Timer.builder("outbox.publish.time").register(registry);

        FunctionCounter.builder("outbox.events.sent", sentTotal, AtomicLong::get).register(registry);
        FunctionCounter.builder("outbox.events.failed", failedTotal, AtomicLong::get).register(registry);
        FunctionCounter.builder("outbox.events.dead", deadTotal, AtomicLong::get).register(registry);
        Gauge.builder("outbox.events.pending", repository,
                        r -> r.countByStatus("NEW") + r.countByStatus("FAILED"))
                .register(registry);
    }

    public void recordSent(Duration elapsed) {
        sentTotal.incrementAndGet();
        publishTimer.record(elapsed);
    }

    public void recordFailed() {
        failedTotal.incrementAndGet();
    }

    public void recordDead() {
        deadTotal.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                repository.countByStatus("NEW") + repository.countByStatus("FAILED"),
                sentTotal.get(),
                failedTotal.get(),
                deadTotal.get()
        );
    }

    public record Snapshot(long pending, long sentTotal, long failedTotal, long deadTotal) {}
}