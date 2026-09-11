package ru.mentee.power.orders.adapters.metrics;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * saga.step.duration — Timer, потому что длительность шага реально варьируется
 * (сеть, нагрузка на внешний сервис) — в отличие от константного TTL из
 * IdempotencyMetricsRegistry (MKAFKA-07 §2.7), где Timer был бы бессмысленным.
 */
@Component
public class SagaMetricsRegistry {

    private final AtomicInteger activeSagas = new AtomicInteger();
    private final AtomicLong compensationTotal = new AtomicLong();
    private final MeterRegistry registry;

    public SagaMetricsRegistry(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("saga.steps.active", activeSagas, AtomicInteger::get).register(registry);
        FunctionCounter.builder("saga.compensation.count", compensationTotal, AtomicLong::get).register(registry);
    }

    public void sagaStarted() {
        activeSagas.incrementAndGet();
    }

    public void sagaFinished() {
        activeSagas.decrementAndGet();
    }

    public void incrementCompensation() {
        compensationTotal.incrementAndGet();
    }

    public Sample startStep(String stepName) {
        return new Sample(stepName, Timer.start(registry));
    }

    public void recordStep(Sample sample) {
        sample.timerSample.stop(Timer.builder("saga.step.duration")
                .tag("step", sample.stepName)
                .register(registry));
    }

    public long compensationTotal() {
        return compensationTotal.get();
    }

    public int activeSagas() {
        return activeSagas.get();
    }

    public record Sample(String stepName, Timer.Sample timerSample) {}
}