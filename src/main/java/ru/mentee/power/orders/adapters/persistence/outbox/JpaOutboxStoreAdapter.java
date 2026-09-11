package ru.mentee.power.orders.adapters.persistence.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OutboxEvent;
import ru.mentee.power.orders.ports.outgoing.OutboxStorePort;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class JpaOutboxStoreAdapter implements OutboxStorePort {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;

    public JpaOutboxStoreAdapter(OutboxEventRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void append(UUID orderId, OrderEventPayload payload) {
        // MKAFKA-07: id строки = eventId доставки, а не независимый UUID.randomUUID() —
        // так outbox_event.id и OrderEventPayload.eventId() физически совпадают
        // на всех повторных попытках публикации (теория §2.1).
        OutboxEventEntity entity = new OutboxEventEntity(
                payload.eventId(), orderId, "OrderCreated", writeJson(payload), Instant.now());
        repository.save(entity);
    }

    @Override
    @Transactional
    public List<OutboxEvent> fetchDueBatch(int batchSize) {
        List<OutboxEventEntity> claimed = repository.lockDueBatch(batchSize);
        claimed.forEach(entity -> entity.setStatus("DISPATCHING"));
        repository.saveAll(claimed);
        return claimed.stream().map(this::toDomain).toList();
    }

    @Override
    public void markSent(UUID outboxEventId) {
        repository.findById(outboxEventId).ifPresent(entity -> {
            entity.setStatus("SENT");
            entity.setSentAt(Instant.now());
            repository.save(entity);
        });
    }

    @Override
    public void markFailed(UUID outboxEventId, String errorMessage, Instant nextRetryAt) {
        repository.findById(outboxEventId).ifPresent(entity -> {
            entity.setStatus("FAILED");
            entity.setAttempts(entity.getAttempts() + 1);
            entity.setNextRetryAt(nextRetryAt);
            entity.setLastError(truncate(errorMessage));
            repository.save(entity);
        });
    }

    @Override
    public void markDead(UUID outboxEventId, String errorMessage) {
        repository.findById(outboxEventId).ifPresent(entity -> {
            entity.setStatus("DEAD");
            entity.setAttempts(entity.getAttempts() + 1);
            entity.setLastError(truncate(errorMessage));
            repository.save(entity);
        });
    }

    @Override
    public int deleteSentOlderThan(Instant threshold) {
        return repository.deleteByStatusSentAndSentAtBefore(threshold);
    }

    private OutboxEvent toDomain(OutboxEventEntity entity) {
        return new OutboxEvent(entity.getId(), entity.getOrderId(), readJson(entity.getPayload()), entity.getAttempts());
    }

    private String writeJson(OrderEventPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize OrderEventPayload for outbox", ex);
        }
    }

    private OrderEventPayload readJson(String json) {
        try {
            return objectMapper.readValue(json, OrderEventPayload.class);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to deserialize outbox payload", ex);
        }
    }

    private String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() > 500 ? message.substring(0, 500) : message;
    }
}