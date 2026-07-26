# MKAFKA-03 — Атлас файлов: Producer REST → Kafka

> Читай вместе с [MKAFKA-03-producer-rest-kafka.md](MKAFKA-03-producer-rest-kafka.md) — там **азбука** (что такое port / adapter / use case / record).  
> Здесь — **как выглядят файлы** в уже написанном коде `src/`.

↑ [Спринт 02](README.md) · [План](../00-план-курса-kafka.md)

---

## Мини-словарь перед файлами

| Слово | Одной фразой | Пример файла |
|-------|--------------|--------------|
| **Domain** | бизнес без HTTP/Kafka | `Order.java` |
| **Use case** | один сценарий («оформи заказ») | `PlaceOrderUseCase.java` |
| **Port** | интерфейс-розетка | `PlaceOrderPort`, `OrderEventPort` |
| **Adapter** | вилка к внешнему миру | `OrderController`, `OrderEventProducer` |
| **DTO** | JSON-форма для HTTP | `OrderRequest` |
| **Command** | чистый вход в use case | `PlaceOrderCommand` (record) |
| **record** | короткий неизменяемый «пакет данных» | `OrderEventPayload` |
| **Producer** | кто пишет в Kafka | `OrderEventProducer` |

---

## 0. Логика потока по файлам

```
OrderRequest.json
    → OrderController.java
    → OrderMapper.java                    (DTO → PlaceOrderCommand)
    → PlaceOrderPort.java                 (incoming)
    → PlaceOrderUseCase.java              (validate, Order, memory)
    → OrderEventPort.java                 (outgoing)
    → OrderEventProducer.java             (implements port)
         ├─ KafkaTopicResolver.java       (priority → topic)
         ├─ OrderEventPayload.java        (value в Kafka)
         └─ ProducerMetricsRegistry.java  (success/failure)
    → KafkaTemplate → брокер
```

**Удали или не используй** `NoOpOrderEventAdapter` — иначе два бина `OrderEventPort` и Spring упадёт на ambiguous dependency. Оставь один: `OrderEventProducer`.

---

## 1. Конфигурация

### 1.1. `application-local.yml` — дополнения

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/orders
    username: orders
    password: orders
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      properties:
        enable.idempotence: true
        spring.json.add.type.headers: false

app:
  kafka:
    health-enabled: true
    topics:
      high: orders.priority.high
      normal: orders.priority.normal
      low: orders.priority.low
```

**Зачем:**

| Ключ | Смысл |
|------|--------|
| `JsonSerializer` | value = объект → JSON в топик |
| `acks: all` | ждём подтверждения реплик (с idempotence обычно вместе) |
| `enable.idempotence` | требование урока |
| `app.kafka.topics.*` | имена не хардкодить в use case |

В `application.yml` можешь оставить общие defaults; топики — в `local`.

---

### 1.2. `adapters/config/KafkaProducerConfig.java`

**Зачем:** бины топиков + при необходимости кастомный `KafkaTemplate`.

```java
package ru.mentee.power.orders.adapters.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("!ci")
public class KafkaProducerConfig {

    @Bean
    NewTopic ordersPriorityHigh(@Value("${app.kafka.topics.high}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic ordersPriorityNormal(@Value("${app.kafka.topics.normal}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic ordersPriorityLow(@Value("${app.kafka.topics.low}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }
}
```

`partitions(3)` — учебный запас под разные region‑ключи. На одном брокере `replicas(1)`.

Старый `KafkaConfig` с одним `order-events` — замени или удали, чтобы не плодить лишний топик.

---

## 2. Domain

### 2.1. `OrderPriority.java` (или enum внутри model)

```java
package ru.mentee.power.orders.domain.model;

public enum OrderPriority {
    HIGH, NORMAL, LOW
}
```

OpenAPI: именно `NORMAL`, не `MEDIUM`.

---

### 2.2. `Order.java` — актуализируй поля

```java
package ru.mentee.power.orders.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Order {
    private UUID id;
    private UUID customerId;
    private String region;
    private OrderPriority priority;
    private BigDecimal amount;
    private OrderStatus status;
    private Instant createdAt;
    private List<OrderLine> lines = new ArrayList<>();

    public static Order createNew(UUID customerId, String region, OrderPriority priority,
                                  BigDecimal amount, List<OrderLine> lines) {
        Order order = new Order();
        order.id = UUID.randomUUID();
        order.customerId = customerId;
        order.region = region;
        order.priority = priority;
        order.amount = amount;
        order.lines = lines;
        order.status = OrderStatus.NEW; // или отдельный QUEUED на уровне ответа API
        order.createdAt = Instant.now();
        return order;
    }

    // геттеры...
}
```

**Зачем фабрика `createNew`:** id и время задаёт домен, не контроллер.

`OrderLine` — как в MKAFKA‑02 (productId, quantity, price; priority на линии больше не обязателен для этого OpenAPI — приоритет на всём заказе).

---

## 3. Ports

### 3.1. `PlaceOrderPort.java` + Command / Result

**Зачем:** домен не зависит от JSON DTO.

```java
package ru.mentee.power.orders.ports.incoming;

import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface PlaceOrderPort {

    PlaceOrderResult place(PlaceOrderCommand command);

    record PlaceOrderCommand(
            UUID customerId,
            String region,
            OrderPriority priority,
            BigDecimal amount,
            List<OrderLine> lines
    ) {}

    record PlaceOrderResult(
            UUID orderId,
            String status,      // "QUEUED"
            Instant dispatchedAt
    ) {}
}
```

Сигнатура из MKAFKA‑02 `Order place(Order)` — **замени** на Command/Result (это эволюция контракта урока 3).

---

### 3.2. `OrderEventPort.java`

```java
package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.adapters.kafka.OrderEventPayload;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.util.concurrent.CompletionStage;

public interface OrderEventPort {

    CompletionStage<Void> publish(OrderEventPayload payload, OrderPriority priority);
}
```

**Нюанс архитектуры:** чистый hexagonal не любит, когда port зависит от класса в `adapters.kafka`. Лучше:

- положить `OrderEventPayload` в `ports.outgoing` или `domain.model.event`,  
  **или**
- порт принимает domain `Order`, а payload собирает уже адаптер.

Для учебного задания часто делают payload рядом с Kafka. Предпочтительнее для границ:

```java
CompletionStage<Void> publishOrderCreated(Order order);
```

а topic/priority резолвит адаптер из `order.getPriority()`. Оба варианта ок, если use case **не** импортирует `KafkaTemplate`.

Ниже в атласе — вариант с **payload в kafka-пакете** и портом `publish(payload, priority)` как в требованиях сдачи.

Чтобы port не зависел от adapters, перенеси record:

`ru.mentee.power.orders.ports.outgoing.OrderEventPayload`

---

## 4. Use case

### `PlaceOrderUseCase.java`

```java
package ru.mentee.power.orders.domain.usecase;

import org.springframework.stereotype.Service;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PlaceOrderUseCase implements PlaceOrderPort {

    private final OrderEventPort orderEventPort;
    private final Map<UUID, Order> inMemoryStore = new ConcurrentHashMap<>();

    public PlaceOrderUseCase(OrderEventPort orderEventPort) {
        this.orderEventPort = orderEventPort;
    }

    @Override
    public PlaceOrderResult place(PlaceOrderCommand command) {
        validate(command);

        Order order = Order.createNew(
                command.customerId(),
                command.region(),
                command.priority(),
                command.amount(),
                command.lines()
        );

        inMemoryStore.put(order.getId(), order);
        // TODO MKAFKA-04: OrderRepository.save(...)

        OrderEventPayload payload = OrderEventPayload.from(order);

        orderEventPort.publish(payload, order.getPriority())
                .toCompletableFuture()
                .join(); // учебный sync wait; можно обработать и завернуть в 500

        return new PlaceOrderResult(order.getId(), "QUEUED", Instant.now());
    }

    private void validate(PlaceOrderCommand command) {
        if (command.region() == null || command.region().isBlank()) {
            throw new IllegalArgumentException("region must not be blank");
        }
        if (command.amount() == null || command.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("amount must be > 0");
        }
        if (command.lines() == null || command.lines().isEmpty()) {
            throw new IllegalArgumentException("lines must not be empty");
        }
        // quantity >= 1 и т.д.
    }
}
```

**Зачем `join()`:** просто для учебного 202/500. В проде чаще async + отдельный статус; для урока синхронно дождаться send — нормально.

Контроллер ловит `IllegalArgumentException` → 400, другие → 500.

---

## 5. Kafka adapter

### 5.1. `OrderEventPayload.java` (лучше в `ports.outgoing`)

```java
package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderEventPayload(
        UUID orderId,
        UUID customerId,
        String region,
        BigDecimal amount,
        OrderPriority priority,
        List<Line> lines,
        Instant emittedAt
) {
    public record Line(UUID productId, int quantity, BigDecimal price) {}

    public static OrderEventPayload from(Order order) {
        List<Line> lines = order.getLines().stream()
                .map(l -> new Line(l.getProductId(), l.getQuantity(), l.getPrice()))
                .toList();
        return new OrderEventPayload(
                order.getId(),
                order.getCustomerId(),
                order.getRegion(),
                order.getAmount(),
                order.getPriority(),
                lines,
                Instant.now()
        );
    }
}
```

**Зачем record:** неизменяемое событие = удобный контракт «что ушло в шину».

---

### 5.2. `KafkaTopicResolver.java`

```java
package ru.mentee.power.orders.adapters.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.OrderPriority;

@Component
public class KafkaTopicResolver {

    private final String high;
    private final String normal;
    private final String low;

    public KafkaTopicResolver(
            @Value("${app.kafka.topics.high}") String high,
            @Value("${app.kafka.topics.normal}") String normal,
            @Value("${app.kafka.topics.low}") String low
    ) {
        this.high = high;
        this.normal = normal;
        this.low = low;
    }

    public String resolve(OrderPriority priority) {
        return switch (priority) {
            case HIGH -> high;
            case NORMAL -> normal;
            case LOW -> low;
        };
    }
}
```

---

### 5.3. `OrderEventProducer.java`

```java
package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.metrics.ProducerMetricsRegistry;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.outgoing.OrderEventPayload;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Component
public class OrderEventProducer implements OrderEventPort {

    private final KafkaTemplate<String, OrderEventPayload> kafkaTemplate;
    private final KafkaTopicResolver topicResolver;
    private final ProducerMetricsRegistry metrics;

    public OrderEventProducer(
            KafkaTemplate<String, OrderEventPayload> kafkaTemplate,
            KafkaTopicResolver topicResolver,
            ProducerMetricsRegistry metrics
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicResolver = topicResolver;
        this.metrics = metrics;
    }

    @Override
    public CompletionStage<Void> publish(OrderEventPayload payload, OrderPriority priority) {
        String topic = topicResolver.resolve(priority);
        String key = payload.region();

        ProducerRecord<String, OrderEventPayload> record =
                new ProducerRecord<>(topic, key, payload);

        CompletableFuture<Void> result = new CompletableFuture<>();

        kafkaTemplate.send(record).whenComplete((sendResult, ex) -> {
            if (ex == null) {
                metrics.success(topic);
                result.complete(null);
            } else {
                metrics.failure(topic);
                result.completeExceptionally(ex);
            }
        });

        return result;
    }
}
```

**Логика:**

| Деталь | Зачем |
|--------|--------|
| `key = region` | партиционирование по региону |
| `whenComplete` | метрики success/failure |
| implements `OrderEventPort` | use case не знает Kafka |

Удали `@Component` с `NoOpOrderEventAdapter` или весь класс.

---

### 5.4. `ProducerMetricsRegistry.java`

```java
package ru.mentee.power.orders.adapters.metrics;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ProducerMetricsRegistry {

    private final AtomicLong totalSuccess = new AtomicLong();
    private final AtomicLong totalFailure = new AtomicLong();
    private final Map<String, AtomicLong> successByTopic = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> failureByTopic = new ConcurrentHashMap<>();

    public void success(String topic) {
        totalSuccess.incrementAndGet();
        successByTopic.computeIfAbsent(topic, t -> new AtomicLong()).incrementAndGet();
    }

    public void failure(String topic) {
        totalFailure.incrementAndGet();
        failureByTopic.computeIfAbsent(topic, t -> new AtomicLong()).incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(
                new Totals(totalSuccess.get(), totalFailure.get()),
                Map.copyOf(toMap(successByTopic)),
                Map.copyOf(toMap(failureByTopic))
        );
    }

    private Map<String, TopicStat> merge() {
        // удобнее собрать в контроллере/DTO: topics[name] = {success, failure}
        ...
    }

    public record Totals(long success, long failure) {}
    public record TopicStat(long success, long failure) {}
    public record Snapshot(Totals totals, Map<String, Long> successByTopic, Map<String, Long> failureByTopic) {}

    private static Map<String, Long> toMap(Map<String, AtomicLong> src) {
        return src.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, e -> e.getValue().get()));
    }
}
```

В контроллере собери JSON ровно как в OpenAPI `ProducerMetricsResponse` (topics → {success, failure}).

---

## 6. Web adapter

### 6.1. DTO

`OrderRequest` — поля как в OpenAPI 0.2.0: `customerId`, `region`, `priority`, `amount`, `lines` (`OrderLineRequest` без priority на линии).

`OrderAcceptedResponse` — `orderId`, `status`, `dispatchedAt` (вместо старого `OrderResponse` для POST; GET заказа можно оставить TODO).

`ErrorResponse` — `code`, `message`.

`ProducerMetricsResponse` — totals + topics.

---

### 6.2. `OrderMapper.java`

```java
public PlaceOrderPort.PlaceOrderCommand toCommand(OrderRequest request) {
    return new PlaceOrderPort.PlaceOrderCommand(
            request.getCustomerId(),
            request.getRegion(),
            OrderPriority.valueOf(request.getPriority()),
            request.getAmount(),
            request.getLines().stream().map(this::toLine).toList()
    );
}
```

**Не** тащи `OrderRequest` в use case.

---

### 6.3. `OrderController.java`

```java
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final PlaceOrderPort placeOrderPort;
    private final OrderMapper mapper;
    private final ProducerMetricsRegistry metrics;

    @PostMapping
    public ResponseEntity<?> create(@RequestBody OrderRequest request) {
        try {
            var result = placeOrderPort.place(mapper.toCommand(request));
            var body = new OrderAcceptedResponse(
                    result.orderId(), result.status(), result.dispatchedAt());
            return ResponseEntity.accepted().body(body); // 202
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest()
                    .body(new ErrorResponse("ORDER_VALIDATION_FAILED", ex.getMessage()));
        } catch (Exception ex) {
            return ResponseEntity.internalServerError()
                    .body(new ErrorResponse("ORDER_DISPATCH_FAILED", ex.getMessage()));
        }
    }

    @GetMapping("/metrics")
    public ProducerMetricsResponse metrics() {
        // собрать из registry.snapshot()
        ...
    }
}
```

**Важно:** mapping `@GetMapping("/metrics")` объяви **до** или отдельно от `@GetMapping("/{orderId}")`, иначе `metrics` перехватится как UUID и даст 400.

---

## 7. Тест

### `PlaceOrderUseCaseTest.java`

```java
package ru.mentee.power.orders.domain.usecase;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderPriority;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort.PlaceOrderCommand;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PlaceOrderUseCaseTest {

    @Mock OrderEventPort orderEventPort;
    @InjectMocks PlaceOrderUseCase useCase;

    @Test
    void place_publishesEvent_whenValid() {
        when(orderEventPort.publish(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        var cmd = new PlaceOrderCommand(
                UUID.randomUUID(),
                "EU",
                OrderPriority.HIGH,
                new BigDecimal("10.00"),
                List.of(line())
        );

        var result = useCase.place(cmd);

        assertEquals("QUEUED", result.status());
        verify(orderEventPort).publish(any(), eq(OrderPriority.HIGH));
    }

    @Test
    void place_rejectsBlankRegion() {
        var cmd = new PlaceOrderCommand(
                UUID.randomUUID(), "  ", OrderPriority.LOW,
                new BigDecimal("10.00"), List.of(line()));
        assertThrows(IllegalArgumentException.class, () -> useCase.place(cmd));
        verifyNoInteractions(orderEventPort);
    }

    private static OrderLine line() {
        OrderLine l = new OrderLine();
        l.setProductId(UUID.randomUUID());
        l.setQuantity(1);
        l.setPrice(new BigDecimal("10.00"));
        return l;
    }
}
```

Профиль `ci` / мок порта — брокер не нужен.

---

## 8. Документация репозитория

| Путь | Действие |
|------|----------|
| `docs/api/order-api.yaml` | версия 0.2.0, POST 202/400/500, GET metrics |
| `docs/examples/metrics-response.json` | пример JSON |
| `docs/screenshots/` | AKHQ: 3 топика + сообщение |
| `README.md` | «Проверка отправки», curl на **8081** |
| `docs/architecture/` | обновить диаграмму (producer) |

---

## 9. Чеклист файлов MKAFKA‑03

- [ ] `OrderPriority`, обновлённый `Order`
- [ ] `PlaceOrderPort` + Command/Result
- [ ] `OrderEventPayload` + обновлённый `OrderEventPort`
- [ ] `PlaceOrderUseCase` без `UnsupportedOperationException`
- [ ] `KafkaTopicResolver`
- [ ] `OrderEventProducer` (@Component), **без** NoOp
- [ ] `ProducerMetricsRegistry`
- [ ] `KafkaProducerConfig` — 3 NewTopic
- [ ] Controller: POST 202 + GET metrics
- [ ] DTO/mapper под OpenAPI 0.2.0
- [ ] `application-local.yml` — topics + idempotence
- [ ] `PlaceOrderUseCaseTest`
- [ ] OpenAPI + README + examples

---

## 10. Частые ошибки

| Ошибка | Почему | Как |
|--------|--------|-----|
| Два бина `OrderEventPort` | NoOp + Producer | удали NoOp |
| `metrics` → 400 как orderId | порядок mapping | `/metrics` отдельным методом, не `/{id}` |
| Key null в AKHQ | забыли region в ProducerRecord | `new ProducerRecord<>(topic, region, payload)` |
| Всё в high | priority не мапится из JSON | `OrderPriority.valueOf` / проверка enum |
| Тест лезет в Kafka | нет мока | `@Mock OrderEventPort` |
| curl :8080 | конфликт с AKHQ | **8081** |

---

## Связь

| Документ | Роль |
|----------|------|
| [MKAFKA-03 урок](MKAFKA-03-producer-rest-kafka.md) | теория и checkpoints |
| Этот атлас | содержимое файлов |
| [MKAFKA-02 атлас](../sprint-01-fundamentals/MKAFKA-02-атлас-файлов.md) | каркас, с которого стартуем |
