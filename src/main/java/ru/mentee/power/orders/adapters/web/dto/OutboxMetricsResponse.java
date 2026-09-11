package ru.mentee.power.orders.adapters.web.dto;

public record OutboxMetricsResponse(long pending, long sentTotal, long failedTotal, long deadTotal) {}