package ru.mentee.power.orders.ports.outgoing;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Исходящий порт: «домену нужно надёжно запомнить намерение опубликовать событие
 * и позже узнать, какие события ещё не доставлены».
 * Реализация — JpaOutboxStoreAdapter (adapters.persistence.outbox).
 */
public interface OutboxStorePort {

    void append(UUID orderId, OrderEventPayload payload);

    /**
     * Забирает и атомарно помечает пачку событий как "в процессе публикации"
     * (см. JpaOutboxStoreAdapter — почему это важно, а не просто SELECT).
     */
    List<OutboxEvent> fetchDueBatch(int batchSize);

    void markSent(UUID outboxEventId);

    void markFailed(UUID outboxEventId, String errorMessage, Instant nextRetryAt);

    void markDead(UUID outboxEventId, String errorMessage);

    int deleteSentOlderThan(Instant threshold);
}