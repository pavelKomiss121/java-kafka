package ru.mentee.power.orders.adapters.persistence.dedup;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.mentee.power.orders.ports.outgoing.DedupStorePort;

import java.time.Instant;
import java.util.UUID;

/**
 * tryReserve не использует нативный INSERT ... ON CONFLICT DO NOTHING — H2 (itest, 2.2.224)
 * не поддерживает этот синтаксис даже в MODE=PostgreSQL (в отличие от FOR UPDATE SKIP LOCKED
 * из OutboxEventRepository), это чистый Postgres-диалект. Вместо dialect-specific SQL —
 * портируемый JPA-идиом: пробуем вставить строку и ловим нарушение составного PK как признак дубля.
 *
 * Вставка выполняется в ОТДЕЛЬНОЙ транзакции (REQUIRES_NEW через TransactionTemplate, не через
 * @Transactional-аннотацию — самовызов метода того же бина не прошёл бы через AOP-прокси).
 * Если бы вставка шла в той же транзакции, что и остальной handle(...), нарушение уникального
 * ограничения пометило бы ВСЮ транзакцию Hibernate как rollback-only — даже поймав исключение
 * здесь, метод handle(...) в конце упал бы с UnexpectedRollbackException при коммите.
 * REQUIRES_NEW изолирует попытку вставки в свою собственную транзакцию/соединение: конфликт
 * откатывает только её, внешняя транзакция guard'ов и бизнес-логики не затрагивается.
 */
@Component
public class JpaDedupStoreAdapter implements DedupStorePort {

    private final ConsumerEventDedupRepository repository;
    private final TransactionTemplate requiresNewTransaction;

    public JpaDedupStoreAdapter(ConsumerEventDedupRepository repository, PlatformTransactionManager transactionManager) {
        this.repository = repository;
        this.requiresNewTransaction = new TransactionTemplate(transactionManager);
        this.requiresNewTransaction.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public boolean tryReserve(UUID orderId, UUID eventId, Instant expiresAt) {
        try {
            requiresNewTransaction.executeWithoutResult(status -> repository.saveAndFlush(
                    new ConsumerEventDedupEntity(orderId, eventId, Instant.now(), expiresAt)));
            return true;
        } catch (DataIntegrityViolationException ex) {
            return false;
        }
    }

    @Override
    public int deleteExpired(Instant threshold) {
        return repository.deleteByExpiresAtBefore(threshold);
    }
}
