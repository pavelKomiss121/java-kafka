package ru.mentee.power.orders.adapters.metrics;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ConsumerMetricsRegistry {

    private final Map<OrderPriority, AtomicLong> processedByPriority = new EnumMap<>(OrderPriority.class);
    private final Map<String, AtomicLong> processedByRegion = new ConcurrentHashMap<>();
    private final Map<OrderPriority, AtomicLong> dlqByPriority = new EnumMap<>(OrderPriority.class);
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong retryAttempts = new AtomicLong();

    public ConsumerMetricsRegistry() {
        for (OrderPriority priority : OrderPriority.values()) {
            processedByPriority.put(priority, new AtomicLong());
            dlqByPriority.put(priority, new AtomicLong());
        }
    }

    public void processed(OrderPriority priority, String region) {
        processedByPriority.get(priority).incrementAndGet();
        processedByRegion.computeIfAbsent(region, r -> new AtomicLong()).incrementAndGet();
    }

    public void duplicate() {
        duplicates.incrementAndGet();
    }

    public void rejected() {
        rejected.incrementAndGet();
    }

    public void retryAttempt() {
        retryAttempts.incrementAndGet();
    }

    public void dlq(OrderPriority priority) {
        dlqByPriority.get(priority).incrementAndGet();
    }

    public Snapshot snapshot() {
        Map<String, Long> byPriority = new LinkedHashMap<>();
        processedByPriority.forEach((priority, count) -> byPriority.put(priority.name(), count.get()));

        Map<String, Long> byRegion = new LinkedHashMap<>();
        processedByRegion.forEach((region, count) -> byRegion.put(region, count.get()));

        Map<String, Long> dlqByPriorityView = new LinkedHashMap<>();
        dlqByPriority.forEach((priority, count) -> dlqByPriorityView.put(priority.name(), count.get()));

        long totalProcessed = processedByPriority.values().stream().mapToLong(AtomicLong::get).sum();
        long totalDlq = dlqByPriority.values().stream().mapToLong(AtomicLong::get).sum();

        return new Snapshot(totalProcessed, duplicates.get(), rejected.get(), retryAttempts.get(),
                totalDlq, byPriority, byRegion, dlqByPriorityView);
    }

    public record Snapshot(
            long processed,
            long duplicates,
            long rejected,
            long retryAttempts,
            long dlqCount,
            Map<String, Long> byPriority,
            Map<String, Long> byRegion,
            Map<String, Long> dlqByPriority
    ) {}
}