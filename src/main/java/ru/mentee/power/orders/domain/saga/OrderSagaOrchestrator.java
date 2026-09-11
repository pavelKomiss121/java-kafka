package ru.mentee.power.orders.domain.saga;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.metrics.SagaMetricsRegistry;
import ru.mentee.power.orders.ports.outgoing.SagaLogPort;

import java.util.ArrayList;
import java.util.List;

/**
 * Прогоняет шаги по порядку; при исключении на любом шаге компенсирует уже
 * ВЫПОЛНЕННЫЕ шаги в обратном порядке и перебрасывает исходное исключение
 * без оборачивания — вызывающий код (OrderConsumerUseCase) решает по типу
 * исключения, DLQ это или CANCELLED (теория §2.4, §2.7).
 */
@Component
public class OrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);

    private final SagaLogPort sagaLogPort;
    private final SagaMetricsRegistry metrics;


    public OrderSagaOrchestrator(SagaLogPort sagaLogPort, SagaMetricsRegistry metrics) {
        this.sagaLogPort = sagaLogPort;
        this.metrics = metrics;
    }

    public void run(List<SagaStep<OrderSagaContext>> steps, OrderSagaContext context){
        metrics.sagaStarted();
        List<SagaStep<OrderSagaContext>> executed = new ArrayList<>();

        try{
            for (SagaStep<OrderSagaContext> step : steps) {
                sagaLogPort.startStep(context.sagaId(), context.orderId(), step.name());
                SagaMetricsRegistry.Sample sample = metrics.startStep(step.name());
                try {
                    step.execute(context);
                    metrics.recordStep(sample);
                } catch (RuntimeException ex) {
                    metrics.recordStep(sample);
                    sagaLogPort.failStep(context.sagaId(), step.name(), ex.getMessage());
                    compensate(executed, context);
                    throw ex;
                }
                sagaLogPort.completeStep(context.sagaId(), step.name());
                executed.add(step);
            }
        } finally {
            metrics.sagaFinished();
        }
    }

    private void compensate(List<SagaStep<OrderSagaContext>> executed, OrderSagaContext context) {
        for (int i = executed.size() - 1; i >= 0; i--) {
            SagaStep<OrderSagaContext> step = executed.get(i);
            try {
                step.compensate(context);
                sagaLogPort.compensateStep(context.sagaId(), step.name(), null);
                metrics.incrementCompensation();
            } catch (RuntimeException compensationError) {
                sagaLogPort.failStep(context.sagaId(), step.name() + ":compensation", compensationError.getMessage());
                log.error("Compensation failed for step {} in saga {}: {}",
                        step.name(), context.sagaId(), compensationError.getMessage(), compensationError);
            }
        }
    }
}
