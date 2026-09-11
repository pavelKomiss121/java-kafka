package ru.mentee.power.orders.adapters.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.ports.incoming.DispatchOutboxPort;

/**
 * Отключается через app.outbox.scheduler-enabled=false в itest/ci —
 * тесты вызывают DispatchOutboxPort вручную, детерминированно, без гонки с таймером.
 */
@Component
@ConditionalOnProperty(name = "app.outbox.scheduler-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublishScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublishScheduler.class);

    private final DispatchOutboxPort dispatchOutboxPort;
    private final int batchSize;

    public OutboxPublishScheduler(
            DispatchOutboxPort dispatchOutboxPort,
            @Value("${app.outbox.batch-size:50}") int batchSize
    ) {
        this.dispatchOutboxPort = dispatchOutboxPort;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:2000}")
    public void publishDueBatch() {
        DispatchOutboxPort.DispatchResult result = dispatchOutboxPort.dispatchDueBatch(batchSize);
        if (result.sent() > 0 || result.failed() > 0 || result.dead() > 0) {
            log.info("Outbox dispatch tick: sent={}, failed={}, dead={}",
                    result.sent(), result.failed(), result.dead());
        }
    }
}