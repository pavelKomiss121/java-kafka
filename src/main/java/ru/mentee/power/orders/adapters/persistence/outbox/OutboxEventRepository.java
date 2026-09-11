package ru.mentee.power.orders.adapters.persistence.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /**
     * FOR UPDATE SKIP LOCKED защищает от того, чтобы два инстанса планировщика
     * одновременно выбрали одну и ту же строку (см. теорию §2.3). Работает только
     * внутри вызывающей @Transactional-транзакции — вне её блокировка не имеет смысла.
     */
    @Query(value = """
            SELECT * FROM outbox_event
            WHERE status = 'NEW'
               OR (status = 'FAILED' AND next_retry_at <= now())
            ORDER BY created_at
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockDueBatch(@Param("batchSize") int batchSize);

    long countByStatus(String status);

    @Modifying
    @Query("DELETE FROM OutboxEventEntity e WHERE e.status = 'SENT' AND e.sentAt < :threshold")
    int deleteByStatusSentAndSentAtBefore(@Param("threshold") Instant threshold);
}