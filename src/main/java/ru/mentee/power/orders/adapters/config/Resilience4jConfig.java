package ru.mentee.power.orders.adapters.config;

import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import io.github.resilience4j.retry.Retry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;

/**
 * Подписывает наши метрики на события Resilience4j: каждая повторная попытка
 * любого зарегистрированного Retry (в проекте — только "order-processing")
 * увеличивает ConsumerMetricsRegistry.retryAttempts. Без этого биндинга ретраи
 * видны только в логах, но не в /consumers/metrics.
 */
@Configuration
public class Resilience4jConfig {

    @Bean
    public RegistryEventConsumer<Retry> retryMetricsRegistryEventConsumer(ConsumerMetricsRegistry metrics) {
        return new RegistryEventConsumer<>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<Retry> event) {
                event.getAddedEntry().getEventPublisher().onRetry(e -> metrics.retryAttempt());
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<Retry> event) {
                // не используется: retry-инстансы в этом проекте не удаляются в рантайме
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<Retry> event) {
                // не используется: конфигурация retry не переопределяется в рантайме
            }
        };
    }
}