package ru.mentee.power.orders.adapters.web.dto;

public record SagaMetricsResponse(int activeSagas, long compensationTotal) {}