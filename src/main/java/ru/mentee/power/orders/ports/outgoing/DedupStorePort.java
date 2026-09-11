package ru.mentee.power.orders.ports.outgoing;

import java.time.Instant;
import java.util.UUID;

/**
 * Исходящий порт: «домену нужно один раз безопасно зарезервировать факт получения
 * конкретной доставки события и знать, была ли она уже обработана».
 * business_key = orderId, event_id = OrderEventPayload.eventId() (теория §2.1/§2.2).
 * Реализация — JpaDedupStoreAdapter (adapters.persistence.dedup).
 */
public interface DedupStorePort {

    /**
     * Атомарно резервирует комбинацию (orderId, eventId).
     * true  — резервирование удалось, это первая обработка (miss).
     * false — комбинация уже существует, это повтор (hit).
     */
    boolean tryReserve(UUID orderId, UUID eventId, Instant expiresAt);

    int deleteExpired(Instant threshold);
}