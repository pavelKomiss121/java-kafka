package ru.mentee.power.orders.adapters.metrics;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Счётчики consumer'а: сколько заказов обработано (по priority/region),
 * сколько дублей пропущено и сколько событий отклонено валидацией.
 */
@Component
public class ConsumerMetricsRegistry {

    private final Map<OrderPriority, AtomicLong> processedByPriority = new EnumMap<>(OrderPriority.class);
    private final Map<String, AtomicLong> processedByRegion = new ConcurrentHashMap<>();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    public ConsumerMetricsRegistry() {
        for (OrderPriority priority : OrderPriority.values()) {
            processedByPriority.put(priority, new AtomicLong());
        }
    }

    public void processed(OrderPriority priority, String region) {
        processedByPriority.get(priority).incrementAndGet();
        processedByRegion.computeIfAbsent(region, r -> new AtomicLong()).incrementAndGet();
    }

    public void duplicate(){
        duplicates.getAndIncrement();
    }
    public void rejected() {
        rejected.incrementAndGet();
    }

    public Snapshot snapshot() {
        Map<String, Long> byPriority = new LinkedHashMap<>();
        processedByPriority.forEach((priority, count) -> byPriority.put(priority.name(), count.get()));

        Map<String, Long> byRegion = new LinkedHashMap<>();
        processedByRegion.forEach((region, count) -> byRegion.put(region, count.get()));

        long totalProcessed = processedByPriority.values().stream().mapToLong(AtomicLong::get).sum();

        return new Snapshot(totalProcessed, duplicates.get(), rejected.get(), byPriority, byRegion);
    }

    public record Snapshot(
            long processed,
            long duplicates,
            long rejected,
            Map<String, Long> byPriority,
            Map<String, Long> byRegion
    ) {}
}
