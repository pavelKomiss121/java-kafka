package ru.mentee.power.orders.ports.outgoing;

import java.util.UUID;

/**
 * Исходящий порт: «оркестратору нужно протоколировать состояние каждого шага».
 * sagaId = payload.eventId() (теория §2.5). Реализация — JpaSagaLogAdapter.
 */
public interface SagaLogPort {

    void startStep(UUID sagaId, UUID orderId, String stepName);

    void completeStep(UUID sagaId, String stepName);

    void failStep(UUID sagaId, String stepName, String errorMessage);

    void compensateStep(UUID sagaId, String stepName, String errorMessage);
}