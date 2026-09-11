package ru.mentee.power.orders.adapters.persistence.saga;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.ports.outgoing.SagaLogPort;

import java.time.Instant;
import java.util.UUID;

@Component
public class JpaSagaLogAdapter implements SagaLogPort {

    private final SagaExecutionLogRepository repository;
    public JpaSagaLogAdapter(SagaExecutionLogRepository repository) {
        this.repository = repository;
    }

    @Override
    public void startStep(UUID sagaId, UUID orderId, String stepName) {
        repository.save(new SagaExecutionLogEntity(sagaId, stepName, orderId, "IN_PROGRESS", Instant.now()));
    }

    @Override
    public void completeStep(UUID sagaId, String stepName) {
        updateStatus(sagaId, stepName, "COMPLETED", null);
    }

    @Override
    public void failStep(UUID sagaId, String stepName, String errorMessage) {
        updateStatus(sagaId, stepName, "FAILED", errorMessage);
    }

    @Override
    public void compensateStep(UUID sagaId, String stepName, String errorMessage) {
        updateStatus(sagaId, stepName, "COMPENSATED", errorMessage);
    }

    private void updateStatus(UUID sagaId, String stepName, String status, String errorMessage) {
        repository.findById(new SagaExecutionLogEntity.Key(sagaId, stepName)).ifPresent(entity -> {
            entity.setStatus(status);
            entity.setErrorMessage(errorMessage);
            entity.setUpdatedAt(Instant.now());
            repository.save(entity);
        });
    }


}
