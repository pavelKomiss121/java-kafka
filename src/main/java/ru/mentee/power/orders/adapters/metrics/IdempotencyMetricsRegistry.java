package ru.mentee.power.orders.adapters.metrics;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.persistence.dedup.ConsumerEventDedupRepository;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Аналог OutboxMetricsRegistry (MKAFKA-06): AtomicLong — источник правды,
 * FunctionCounter/Gauge — проекция в Micrometer для /actuator/prometheus.
 * Нет Timer ttl_seconds — TTL константа конфигурации, а не переменная
 * величина конкретного события (теория §2.7).
 */
@Component
public class IdempotencyMetricsRegistry {

    private final AtomicLong hitTotal = new AtomicLong();
    private final AtomicLong missTotal = new AtomicLong();
    private final AtomicLong evictedTotal = new AtomicLong();
    private final ConsumerEventDedupRepository repository;

    public IdempotencyMetricsRegistry(MeterRegistry registry, ConsumerEventDedupRepository repository) {
        this.repository = repository;

        FunctionCounter.builder("idempotency.dedup.hit", hitTotal, AtomicLong::get).register(registry);
        FunctionCounter.builder("idempotency.dedup.miss", missTotal, AtomicLong::get).register(registry);
        FunctionCounter.builder("idempotency.dedup.evicted", evictedTotal, AtomicLong::get).register(registry);
        Gauge.builder("idempotency.dedup.active", repository,
                        r -> r.countByExpiresAtAfter(Instant.now()))
                .register(registry);
    }

    public void hit() {
        hitTotal.incrementAndGet();
    }

    public void miss() {
        missTotal.incrementAndGet();
    }

    public void evicted(long count) {
        evictedTotal.addAndGet(count);
    }

    public Snapshot snapshot() {
        return new Snapshot(hitTotal.get(), missTotal.get(), evictedTotal.get());
    }

    public record Snapshot(long hitTotal, long missTotal, long evictedTotal) {}
}