package ru.mentee.power.orders.adapters.web.dto;

public record IdempotencyMetricsResponse(long hitTotal, long missTotal, long evictedTotal) {}