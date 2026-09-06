package ru.mentee.power.orders.adapters.web.dto;

import java.util.Map;

public record ConsumerMetricsResponse(
        long processed,
        long duplicates,
        long rejected,
        Map<String, Long> byPriority,
        Map<String, Long> byRegion
) {}