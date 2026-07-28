package ru.mentee.power.orders.adapters.web;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.mentee.power.orders.adapters.metrics.ProducerMetricsRegistry;
import ru.mentee.power.orders.adapters.web.dto.ErrorResponse;
import ru.mentee.power.orders.adapters.web.dto.OrderAcceptedResponse;
import ru.mentee.power.orders.adapters.web.dto.OrderRequest;
import ru.mentee.power.orders.adapters.web.dto.OrderResponse;
import ru.mentee.power.orders.adapters.web.dto.ProducerMetricsResponse;
import ru.mentee.power.orders.adapters.web.mapper.OrderMapper;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;

/**
 * HTTP-адаптер: только транспорт. Бизнес — через PlaceOrderPort.
 */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final PlaceOrderPort placeOrderPort;
    private final OrderMapper orderMapper;
    private final ProducerMetricsRegistry metricsRegistry;

    public OrderController(
            PlaceOrderPort placeOrderPort,
            OrderMapper orderMapper,
            ProducerMetricsRegistry metricsRegistry
    ) {
        this.placeOrderPort = placeOrderPort;
        this.orderMapper = orderMapper;
        this.metricsRegistry = metricsRegistry;
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
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            return ResponseEntity.internalServerError()
                    .body(new ErrorResponse("ORDER_DISPATCH_FAILED", cause.getMessage()));
        } catch (Exception ex) {
            return ResponseEntity.internalServerError()
                    .body(new ErrorResponse("ORDER_DISPATCH_FAILED", ex.getMessage()));
        }
    }

    /**
     * Метрики объявлены ДО /{orderId}, иначе Spring примет "metrics" как UUID.
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
        // TODO MKAFKA-04: читать из БД
        return ResponseEntity.status(501).build();
    }
}
