package ru.mentee.power.orders.adapters.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.mentee.power.orders.adapters.metrics.ConsumerMetricsRegistry;
import ru.mentee.power.orders.adapters.metrics.IdempotencyMetricsRegistry;
import ru.mentee.power.orders.adapters.metrics.OutboxMetricsRegistry;
import ru.mentee.power.orders.adapters.metrics.ProducerMetricsRegistry;
import ru.mentee.power.orders.adapters.web.dto.*;
import ru.mentee.power.orders.adapters.web.mapper.OrderMapper;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final PlaceOrderPort placeOrderPort;
    private final OrderMapper orderMapper;
    private final ProducerMetricsRegistry metricsRegistry;
    private final ConsumerMetricsRegistry consumerMetricsRegistry;
    private final OutboxMetricsRegistry outboxMetricsRegistry;
    private final IdempotencyMetricsRegistry idempotencyMetricsRegistry;

    public OrderController(
            PlaceOrderPort placeOrderPort,
            OrderMapper orderMapper,
            ProducerMetricsRegistry metricsRegistry,
            ConsumerMetricsRegistry consumerMetricsRegistry,
            OutboxMetricsRegistry outboxMetricsRegistry,
            IdempotencyMetricsRegistry idempotencyMetricsRegistry
    ) {
        this.placeOrderPort = placeOrderPort;
        this.orderMapper = orderMapper;
        this.metricsRegistry = metricsRegistry;
        this.consumerMetricsRegistry = consumerMetricsRegistry;
        this.outboxMetricsRegistry = outboxMetricsRegistry;
        this.idempotencyMetricsRegistry = idempotencyMetricsRegistry;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody OrderRequest request) {
        try {
            PlaceOrderPort.PlaceOrderResult result = placeOrderPort.place(orderMapper.toCommand(request));
            OrderAcceptedResponse body = orderMapper.toAccepted(result);
            return ResponseEntity.accepted().body(body);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest()
                    .body(new ErrorResponse("ORDER_VALIDATION_FAILED", ex.getMessage()));
        } catch (Exception ex) {
            return ResponseEntity.internalServerError()
                    .body(new ErrorResponse("ORDER_DISPATCH_FAILED", ex.getMessage()));
        }
    }

    @GetMapping("/outbox/metrics")
    public OutboxMetricsResponse outboxMetrics() {
        OutboxMetricsRegistry.Snapshot snapshot = outboxMetricsRegistry.snapshot();
        return new OutboxMetricsResponse(
                snapshot.pending(), snapshot.sentTotal(), snapshot.failedTotal(), snapshot.deadTotal());
    }

    /**
     * Метрики idempotency: сколько доставок отсеяно как дубль / обработано впервые /
     * вычищено по TTL, и сколько строк сейчас активно в consumer_event_dedup.
     */
    @GetMapping("/idempotency/metrics")
    public IdempotencyMetricsResponse idempotencyMetrics() {
        IdempotencyMetricsRegistry.Snapshot snapshot = idempotencyMetricsRegistry.snapshot();
        return new IdempotencyMetricsResponse(
                snapshot.hitTotal(), snapshot.missTotal(), snapshot.evictedTotal());
    }

    @GetMapping("/consumers/metrics")
    public ConsumerMetricsResponse consumerMetrics() {
        ConsumerMetricsRegistry.Snapshot snapshot = consumerMetricsRegistry.snapshot();
        return new ConsumerMetricsResponse(
                snapshot.processed(),
                snapshot.duplicates(),
                snapshot.rejected(),
                snapshot.retryAttempts(),
                snapshot.dlqCount(),
                snapshot.byPriority(),
                snapshot.byRegion(),
                snapshot.dlqByPriority()
        );
    }

    /**
     * Метрики объявлены ДО /{orderId}, иначе Spring примет "metrics"/"outbox"/"idempotency" как UUID.
     */
    @GetMapping("/metrics")
    public ProducerMetricsResponse metrics() {
        ProducerMetricsRegistry.Snapshot snapshot = metricsRegistry.snapshot();
        Map<String, ProducerMetricsResponse.TopicMetrics> topics = new LinkedHashMap<>();
        snapshot.topics().forEach((name, stat) ->
                topics.put(name, new ProducerMetricsResponse.TopicMetrics(stat.success(), stat.failure())));

        return new ProducerMetricsResponse(
                new ProducerMetricsResponse.Totals(
                        snapshot.totals().success(),
                        snapshot.totals().failure()
                ),
                topics
        );
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable UUID orderId) {
        // TODO MKAFKA-07: читать из БД — НЕ относится к идемпотентности, отдельная задача
        return ResponseEntity.status(501).build();
    }
}