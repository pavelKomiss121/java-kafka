package ru.mentee.power.orders.adapters.persistence.dedup;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface ConsumerEventDedupRepository
        extends JpaRepository<ConsumerEventDedupEntity, ConsumerEventDedupEntity.Key> {

    long countByExpiresAtAfter(Instant threshold);

    @Modifying
    @Query("DELETE FROM ConsumerEventDedupEntity e WHERE e.expiresAt < :threshold")
    int deleteByExpiresAtBefore(@Param("threshold") Instant threshold);
}
