package ru.mentee.power.orders.domain.saga;

public interface SagaStep<C> {

    void execute(C context);

    void compensate(C context);

    String name();
}