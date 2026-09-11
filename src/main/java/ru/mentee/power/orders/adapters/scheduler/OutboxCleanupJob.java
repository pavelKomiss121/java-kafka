package ru.mentee.power.orders.adapters.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.ports.outgoing.OutboxStorePort;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
public class OutboxCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleanupJob.class);

    private final OutboxStorePort outboxStorePort;
    private final int retentionDays;

    public OutboxCleanupJob(
            OutboxStorePort outboxStorePort,
            @Value("${app.outbox.cleanup.retention-days:7}") int retentionDays
    ) {
        this.outboxStorePort = outboxStorePort;
        this.retentionDays = retentionDays;
    }

    @Scheduled(cron = "${app.outbox.cleanup.cron:0 0 3 * * *}")
    public void cleanupSentEvents() {
        Instant threshold = Instant.now().minus(retentionDays, ChronoUnit.DAYS);
        int deleted = outboxStorePort.deleteSentOlderThan(threshold);
        if (deleted > 0) {
            log.info("Outbox cleanup: removed {} SENT events older than {} days", deleted, retentionDays);
        }
    }
}