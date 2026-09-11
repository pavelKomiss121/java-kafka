package ru.mentee.power.orders.adapters.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.metrics.IdempotencyMetricsRegistry;
import ru.mentee.power.orders.ports.outgoing.DedupStorePort;

import java.time.Instant;

/**
 * Без @ConditionalOnProperty-гейта — как и OutboxCleanupJob (MKAFKA-06), ежесуточный
 * cron физически не может сработать за время обычного тестового прогона, в отличие
 * от секундного OutboxPublishScheduler (теория §2.8, самопроверка №7).
 */
@Component
public class DedupCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(DedupCleanupJob.class);

    private final DedupStorePort dedupStorePort;
    private final IdempotencyMetricsRegistry metrics;

    public DedupCleanupJob(DedupStorePort dedupStorePort, IdempotencyMetricsRegistry metrics) {
        this.dedupStorePort = dedupStorePort;
        this.metrics = metrics;
    }

    @Scheduled(cron = "${app.idempotency.cleanup.cron:0 15 3 * * *}")
    public void cleanupExpiredRecords() {
        int deleted = dedupStorePort.deleteExpired(Instant.now());
        if (deleted > 0) {
            metrics.evicted(deleted);
            log.info("Dedup cleanup: removed {} expired consumer_event_dedup rows", deleted);
        }
    }
}