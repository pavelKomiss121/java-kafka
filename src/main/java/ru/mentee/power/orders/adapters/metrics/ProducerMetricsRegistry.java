package ru.mentee.power.orders.adapters.metrics;

import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Счётчики успешных/ошибочных отправок в Kafka (по топикам и всего).
 */
@Component
public class ProducerMetricsRegistry {

    private final AtomicLong totalSuccess = new AtomicLong();
    private final AtomicLong totalFailure = new AtomicLong();
    private final Map<String, AtomicLong> successByTopic = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> failureByTopic = new ConcurrentHashMap<>();

    public void success(String topic) {
        totalSuccess.incrementAndGet();
        successByTopic.computeIfAbsent(topic, t -> new AtomicLong()).incrementAndGet();
    }

    public void failure(String topic) {
        totalFailure.incrementAndGet();
        failureByTopic.computeIfAbsent(topic, t -> new AtomicLong()).incrementAndGet();
    }

    public Snapshot snapshot() {
        Map<String, TopicStat> topics = new LinkedHashMap<>();
        Stream.concat(successByTopic.keySet().stream(), failureByTopic.keySet().stream())
                .distinct()
                .sorted()
                .forEach(topic -> topics.put(
                        topic,
                        new TopicStat(
                                successByTopic.getOrDefault(topic, new AtomicLong()).get(),
                                failureByTopic.getOrDefault(topic, new AtomicLong()).get()
                        )
                ));

        return new Snapshot(
                new Totals(totalSuccess.get(), totalFailure.get()),
                topics
        );
    }

    public record Totals(long success, long failure) {}

    public record TopicStat(long success, long failure) {}

    public record Snapshot(Totals totals, Map<String, TopicStat> topics) {}
}
