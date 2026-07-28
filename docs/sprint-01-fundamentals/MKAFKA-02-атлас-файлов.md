# MKAFKA-02 — Атлас файлов: как выглядит каждый файл и зачем он

> Читай вместе с [MKAFKA-02-каркас-mcloud-orders.md](MKAFKA-02-каркас-mcloud-orders.md).  
> Здесь — **конкретный вид файлов** и **логика**: почему файл лежит именно здесь, кто кого вызывает, что делать в MKAFKA‑02, а что оставить на MKAFKA‑03.

↑ [Спринт 01](README.md) · [План курса](../00-план-курса-kafka.md)

Код ниже — **учебные заглушки**. Их можно набрать руками, чтобы проект собирался. Реальную бизнес‑логику и `KafkaTemplate.send` не пиши — помечай `TODO MKAFKA-03`.

---

## 0. Сначала логика, потом файлы

Один будущий запрос (когда всё заработает в MKAFKA‑03) идёт так:

```
1. HTTP JSON          → OrderController.java          (adapters/web)
2. DTO → domain       → OrderMapper.java              (adapters/web)
3. «оформи заказ»     → PlaceOrderPort.java           (ports/incoming)  ← интерфейс
4. бизнес-правила     → PlaceOrderUseCase.java        (domain/usecase)  ← реализует порт
5. сохранить в БД     → OrderRepository.java          (adapters/persistence)
6. «опубликуй событие»→ OrderEventPort.java           (ports/outgoing)  ← интерфейс
7. запись в Kafka     → KafkaOrderEventAdapter.java   (adapters/kafka)  ← в MKAFKA-03
```

**Правило раскладки:**


| Вопрос                                                | Куда класть файл                          |
| ----------------------------------------------------- | ----------------------------------------- |
| Это правило CRM про заказ, без Spring/Kafka?          | `domain/`                                 |
| Это «контракт» (интерфейс), что умеем снаружи/внутрь? | `ports/`                                  |
| Это HTTP, Kafka, БД, конфиг Spring?                   | `adapters/`                               |
| Это настройки окружения?                              | `src/main/resources/`                     |
| Это инфраструктура Docker?                            | корень репо (`docker-compose.yml`)        |
| Это сборка проекта?                                   | `build.gradle.kts`, `settings.gradle.kts` |


**Почему нельзя всё в одном пакете?**  
Если `OrderController` сразу дергает `KafkaTemplate`, ты навсегда смешаешь HTTP и Kafka. Тесты, замена брокера, новый вход (CLI) — всё станет болью. Порты — «розетки», адаптеры — «вилки».

---



## 1. Файлы сборки (корень проекта)



### 1.1. `settings.gradle.kts`

**Зачем:** говорит Gradle, как называется проект.

```kotlin
rootProject.name = "mcloud-orders"
```

Имя должно совпадать с сервисом курса (`mcloud-orders`).

---



### 1.2. `build.gradle.kts`

**Зачем:** список «из чего собран сервис». Без зависимостей не будет ни REST, ни Kafka‑клиента, ни JPA.

```kotlin
plugins {
    java
    id("org.springframework.boot") version "3.2.5"
    id("io.spring.dependency-management") version "1.1.5"
}

group = "ru.mentee.power"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // HTTP API
    implementation("org.springframework.boot:spring-boot-starter-web")

    // Kafka client (Template, AdminClient, NewTopic)
    implementation("org.springframework.kafka:spring-kafka")

    // /actuator/health, info, probes
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // БД + репозитории
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    runtimeOnly("org.postgresql:postgresql:42.7.3")

    // Миграции схемы
    implementation("org.liquibase:liquibase-core")

    // Тесты
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    // для профиля ci — in-memory БД
    testRuntimeOnly("com.h2database:h2")
    runtimeOnly("com.h2database:h2")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
```

**Логика зависимостей:**


| Строка                    | Без неё не будет…                |
| ------------------------- | -------------------------------- |
| `starter-web`             | контроллеров и Tomcat            |
| `spring-kafka`            | подключения к брокеру            |
| `actuator`                | `/actuator/health`               |
| `data-jpa` + `postgresql` | работы с таблицей `orders`       |
| `liquibase-core`          | версионирования схемы            |
| `h2`                      | профиля `ci` без Docker Postgres |


`gradlew` / `gradlew.bat` появятся из Spring Initializr или `gradle wrapper` — их **не пиши руками**, сгенерируй.

---



## 2. Конфигурация Spring (`src/main/resources`)



### 2.1. `application.yml` — общие правила на все профили

**Зачем:** то, что одинаково везде (имя приложения, порт, путь Liquibase, что светит Actuator). Секреты и URL БД — в профилях.

```yaml
spring:
  application:
    name: mcloud-orders
  jpa:
    hibernate:
      ddl-auto: validate   # схему меняет Liquibase, не Hibernate
    open-in-view: false
    show-sql: true
  liquibase:
    change-log: classpath:liquibase/changelog-master.yml
    enabled: true
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

server:
  port: 8081   # 8080 занят AKHQ из MKAFKA-01

management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      show-details: always
      probes:
        enabled: true

info:
  app:
    name: mcloud-orders
    description: Order service for m-CRM Kafka course
    team: mentee-power

# своё свойство — имя топика (не orders.events!)
app:
  kafka:
    topic: order-events
```

**Почему** `ddl-auto=validate`**:** Hibernate только проверяет, что Java‑сущности совпадают с таблицами. Создаёт таблицы **Liquibase** — иначе на проде схема «плывёт» незаметно.

**Почему топик в** `app.kafka.topic`**:** одно место правды; `KafkaConfig` и health читают отсюда.

---



### 2.2. `application-local.yml` — твой ноутбук

**Зачем:** пароли и URL локального Docker Postgres. Включается так:

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/orders
    username: orders
    password: orders   # должно совпадать с docker-compose!
  jpa:
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect

  kafka:
    bootstrap-servers: localhost:9092
```

**Логика:** с **хоста** (IDE / `bootRun`) всегда `localhost`, не `kafka` и не `postgres` — эти имена видны только **внутри** Docker‑сети.

---



### 2.3. `application-ci.yml` — GitHub Actions без Docker

**Зачем:** `./gradlew test` на CI не должен ждать живой Kafka/Postgres.

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:orders;MODE=PostgreSQL;DB_CLOSE_DELAY=-1
    username: sa
    password:
    driver-class-name: org.h2.Driver
  jpa:
    hibernate:
      ddl-auto: none
    properties:
      hibernate:
        dialect: org.hibernate.dialect.H2Dialect
  liquibase:
    enabled: true
  autoconfigure:
    exclude:
      - org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration

# health kafka в ci можно отключить отдельным флагом в коде
app:
  kafka:
    health-enabled: false
```

**Логика:** CI проверяет, что **код компилируется и юнит‑тесты зелёные**, а не что у тебя дома поднят Docker.

---



### 2.4. `application-test.yml` (по желанию)

Похож на `ci`, для локального `./gradlew test`. Можно просто `@ActiveProfiles("ci")` в тестах.

---



## 3. Liquibase — схема БД как код



### 3.1. `liquibase/changelog-master.yml`

**Зачем:** оглавление миграций. Spring читает только master, master подключает SQL‑файлы по порядку.

```yaml
databaseChangeLog:
  - include:
      file: liquibase/changesets/20250220-create-orders-table.sql
```

Путь **относительно classpath** (`src/main/resources/`).

---



### 3.2. `liquibase/changesets/20250220-create-orders-table.sql`

**Зачем:** первый changeset — таблица заказов. Без неё GET/POST в будущем некуда сохранять состояние (Kafka хранит *события*, БД — *текущий статус*).

```sql
--liquibase formatted sql

--changeset mentee:20250220-create-orders
CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    amount      NUMERIC(19, 2) NOT NULL,
    status      VARCHAR(32) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_orders_status ON orders (status);
CREATE INDEX idx_orders_created_at ON orders (created_at);

--rollback DROP TABLE orders;
```


| Элемент                           | Зачем                                        |
| --------------------------------- | -------------------------------------------- |
| `--liquibase formatted sql`       | Liquibase понимает файл как changeset        |
| `--changeset author:id`           | уникальный id миграции (не запускать дважды) |
| индексы по `status`, `created_at` | типичные фильтры в CRM                       |
| `--rollback`                      | как откатить миграцию                        |


После старта приложения с `local` зайди в pgAdmin → база `orders` → таблица `orders`.

---



## 4. Java: точка входа



### `McloudOrdersApplication.java`

**Путь:** `src/main/java/ru/mentee/power/orders/McloudOrdersApplication.java`

**Зачем:** Spring находит этот класс и поднимает контекст (сканирует пакеты **ниже** `ru.mentee.power.orders`).

```java
package ru.mentee.power.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class McloudOrdersApplication {

    public static void main(String[] args) {
        SpringApplication.run(McloudOrdersApplication.class, args);
    }
}
```

В логах должно быть: `Started McloudOrdersApplication`.

---



## 5. Domain — «мозг» (без Kafka и Spring Web)



### 5.1. `OrderStatus.java`

**Зачем:** допустимые статусы заказа = enum из OpenAPI.

```java
package ru.mentee.power.orders.domain.model;

public enum OrderStatus {
    NEW,
    PROCESSING,
    PAID,
    SHIPPED,
    CANCELLED
}
```

---



### 5.2. `OrderLine.java`

**Зачем:** позиция заказа (товар, количество, цена, приоритет). Это **domain**, не DTO — без Jackson‑аннотаций.

```java
package ru.mentee.power.orders.domain.model;

import java.math.BigDecimal;
import java.util.UUID;

public class OrderLine {

    private UUID productId;
    private int quantity;
    private BigDecimal price;
    private Priority priority;

    public enum Priority {
        HIGH, NORMAL, LOW
    }

    // TODO MKAFKA-03: конструктор / фабрика / геттеры по необходимости
    public UUID getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public BigDecimal getPrice() { return price; }
    public Priority getPriority() { return priority; }

    public void setProductId(UUID productId) { this.productId = productId; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
    public void setPrice(BigDecimal price) { this.price = price; }
    public void setPriority(Priority priority) { this.priority = priority; }
}
```

---



### 5.3. `Order.java`

**Зачем:** агрегат «заказ» — то, о чём думает CRM. Не путать с `OrderRequest` (это JSON снаружи).

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
    private BigDecimal amount;
    private OrderStatus status;
    private Instant createdAt;
    private List<OrderLine> lines = new ArrayList<>();

    // TODO MKAFKA-03: фабрика create(...), смена статуса
    public UUID getId() { return id; }
    public UUID getCustomerId() { return customerId; }
    public BigDecimal getAmount() { return amount; }
    public OrderStatus getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
    public List<OrderLine> getLines() { return lines; }

    public void setId(UUID id) { this.id = id; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public void setStatus(OrderStatus status) { this.status = status; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public void setLines(List<OrderLine> lines) { this.lines = lines; }
}
```

**Почему отдельно от JPA‑entity?**  
В идеале domain чистый. На практике в учебных проектах часто делают одну JPA‑сущность в `adapters.persistence`. Для MKAFKA‑02 достаточно domain‑модели + интерфейс репозитория; маппинг в таблицу — в MKAFKA‑03. Если хочешь сразу стартовать — можно добавить `OrderEntity` рядом с репозиторием (см. §7).

---



### 5.4. `PlaceOrderUseCase.java`

**Зачем:** сценарий «оформить заказ». Реализует **incoming port**. Пока — заглушка.

```java
package ru.mentee.power.orders.domain.usecase;

import org.springframework.stereotype.Service;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;
// репозиторий подключишь в MKAFKA-03

@Service
public class PlaceOrderUseCase implements PlaceOrderPort {

    private final OrderEventPort orderEventPort;
    // private final OrderRepository orderRepository; // TODO MKAFKA-03

    public PlaceOrderUseCase(OrderEventPort orderEventPort) {
        this.orderEventPort = orderEventPort;
    }

    @Override
    public Order place(Order order) {
        // TODO MKAFKA-03:
        // 1) валидация
        // 2) status = NEW, id = UUID.randomUUID()
        // 3) сохранить в БД
        // 4) orderEventPort.publish(...)
        // 5) вернуть Order
        throw new UnsupportedOperationException("TODO MKAFKA-03: implement place order");
    }
}
```

**Логика:** use case **знает** про `OrderEventPort` (розетку), но **не знает** про `KafkaTemplate` (конкретную вилку).

> `@Service` — небольшой компромисс: Spring создаст бин. Альтернатива — `@Component` / явный `@Bean`. Чистый hexagonal без Spring‑аннотаций в domain сложнее для старта — для курса ок.

---



## 6. Ports — контракты



### 6.1. `PlaceOrderPort.java` (вход)

**Зачем:** «снаружи можно попросить оформить заказ». Контроллер зависит от **интерфейса**, не от класса use case напрямую (удобно для тестов).

```java
package ru.mentee.power.orders.ports.incoming;

import ru.mentee.power.orders.domain.model.Order;

public interface PlaceOrderPort {

    Order place(Order order);
}
```

---



### 6.2. `OrderEventPort.java` (выход)

**Зачем:** «домену нужно опубликовать событие». В MKAFKA‑03 адаптер Kafka реализует этот интерфейс.

```java
package ru.mentee.power.orders.ports.outgoing;

import ru.mentee.power.orders.domain.model.Order;

public interface OrderEventPort {

    void publishOrderCreated(Order order);

    // позже: publishStatusChanged(...)
}
```

**Заглушка на MKAFKA‑02**, чтобы контекст поднялся (use case требует бин):

```java
package ru.mentee.power.orders.adapters.kafka;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.outgoing.OrderEventPort;

@Component
public class NoOpOrderEventAdapter implements OrderEventPort {

    @Override
    public void publishOrderCreated(Order order) {
        // TODO MKAFKA-03: KafkaTemplate.send(topic, orderId, payload)
    }
}
```

Положи файл в `adapters/kafka/` — это уже адаптер, просто пустой.

---



## 7. Adapters — web



### 7.1. DTO: `OrderRequest.java` / `OrderResponse.java`

**Зачем:** формат JSON **ровно как в OpenAPI**. Domain и DTO — разные миры.

```java
package ru.mentee.power.orders.adapters.web.dto;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public class OrderRequest {

    private UUID customerId;
    private BigDecimal amount;
    private List<OrderLineDto> lines;

    public UUID getCustomerId() { return customerId; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public List<OrderLineDto> getLines() { return lines; }
    public void setLines(List<OrderLineDto> lines) { this.lines = lines; }

    public static class OrderLineDto {
        private UUID productId;
        private int quantity;
        private BigDecimal price;
        private String priority; // HIGH | NORMAL | LOW

        public UUID getProductId() { return productId; }
        public void setProductId(UUID productId) { this.productId = productId; }
        public int getQuantity() { return quantity; }
        public void setQuantity(int quantity) { this.quantity = quantity; }
        public BigDecimal getPrice() { return price; }
        public void setPrice(BigDecimal price) { this.price = price; }
        public String getPriority() { return priority; }
        public void setPriority(String priority) { this.priority = priority; }
    }
}
```

```java
package ru.mentee.power.orders.adapters.web.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public class OrderResponse {

    private UUID orderId;
    private String status;
    private BigDecimal amount;
    private Instant createdAt;

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
```

---



### 7.2. `OrderMapper.java`

**Зачем:** перевод DTO ↔ domain. Контроллер не должен вручную копировать 10 полей.

```java
package ru.mentee.power.orders.adapters.web.mapper;

import org.springframework.stereotype.Component;
import ru.mentee.power.orders.adapters.web.dto.OrderRequest;
import ru.mentee.power.orders.adapters.web.dto.OrderResponse;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.domain.model.OrderLine;
import ru.mentee.power.orders.domain.model.OrderStatus;

import java.util.stream.Collectors;

@Component
public class OrderMapper {

    public Order toDomain(OrderRequest request) {
        Order order = new Order();
        order.setCustomerId(request.getCustomerId());
        order.setAmount(request.getAmount());
        if (request.getLines() != null) {
            order.setLines(request.getLines().stream().map(this::toLine).collect(Collectors.toList()));
        }
        return order;
    }

    public OrderResponse toResponse(Order order) {
        OrderResponse response = new OrderResponse();
        response.setOrderId(order.getId());
        response.setStatus(order.getStatus() != null ? order.getStatus().name() : null);
        response.setAmount(order.getAmount());
        response.setCreatedAt(order.getCreatedAt());
        return response;
    }

    private OrderLine toLine(OrderRequest.OrderLineDto dto) {
        OrderLine line = new OrderLine();
        line.setProductId(dto.getProductId());
        line.setQuantity(dto.getQuantity());
        line.setPrice(dto.getPrice());
        if (dto.getPriority() != null) {
            line.setPriority(OrderLine.Priority.valueOf(dto.getPriority()));
        }
        return line;
    }
}
```

---



### 7.3. `OrderController.java`

**Зачем:** HTTP‑вход по OpenAPI. Сейчас можно вернуть **501 Not Implemented** или пробросить в use case (который кинет UnsupportedOperationException).

```java
package ru.mentee.power.orders.adapters.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.mentee.power.orders.adapters.web.dto.OrderRequest;
import ru.mentee.power.orders.adapters.web.dto.OrderResponse;
import ru.mentee.power.orders.adapters.web.mapper.OrderMapper;
import ru.mentee.power.orders.domain.model.Order;
import ru.mentee.power.orders.ports.incoming.PlaceOrderPort;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final PlaceOrderPort placeOrderPort;
    private final OrderMapper orderMapper;

    public OrderController(PlaceOrderPort placeOrderPort, OrderMapper orderMapper) {
        this.placeOrderPort = placeOrderPort;
        this.orderMapper = orderMapper;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@RequestBody OrderRequest request) {
        // TODO MKAFKA-03: полноценный сценарий → 202 Accepted
        Order domain = orderMapper.toDomain(request);
        Order created = placeOrderPort.place(domain); // пока упадёт с UnsupportedOperationException
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(orderMapper.toResponse(created));
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable UUID orderId) {
        // TODO MKAFKA-03: читать из OrderRepository
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
    }
}
```

**Логика зависимостей контроллера:** только `PlaceOrderPort` + mapper. **Нет** `KafkaTemplate` в контроллере.

---



## 8. Adapters — persistence



### `OrderRepository.java`

**Зачем:** доступ к таблице `orders`. Spring Data сам сделает реализацию, если есть entity.

На MKAFKA‑02 достаточно интерфейса + простой entity (иначе JPA не к чему привязать):

```java
package ru.mentee.power.orders.adapters.persistence;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private UUID customerId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(nullable = false, length = 32)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    // геттеры/сеттеры
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public UUID getCustomerId() { return customerId; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
}
```

```java
package ru.mentee.power.orders.adapters.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
    // TODO MKAFKA-03: при необходимости findByStatus(...)
}
```

**Логика:** колонки entity **должны совпасть** с Liquibase‑таблицей, иначе `ddl-auto=validate` упадёт при старте.

---



## 9. Adapters — Kafka



### 9.1. `KafkaConfig.java`

**Зачем:** сказать Spring: «вот имя топика, создай его при старте, если нет».

```java
package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic orderEventsTopic(
            @Value("${app.kafka.topic}") String topicName
    ) {
        return TopicBuilder.name(topicName)
                .partitions(1)
                .replicas(1)   // локальный один брокер
                .build();
    }
}
```

После `bootRun` с живой Kafka топик `order-events` появится в AKHQ.

В профиле `ci`, где Kafka auto-config выключен, этот бин может мешать — оберни в `@Profile("!ci")` при необходимости:

```java
@Profile("!ci")
@Configuration
public class KafkaConfig { ... }
```

---



### 9.2. `KafkaHealthIndicator.java`

**Зачем:** `/actuator/health` показывает, жив ли брокер и есть ли топик.

```java
package ru.mentee.power.orders.adapters.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "app.kafka.health-enabled", havingValue = "true", matchIfMissing = true)
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaAdmin kafkaAdmin;
    private final String topic;

    public KafkaHealthIndicator(
            KafkaAdmin kafkaAdmin,
            @Value("${app.kafka.topic}") String topic
    ) {
        this.kafkaAdmin = kafkaAdmin;
        this.topic = topic;
    }

    @Override
    public Health health() {
        try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // 1) брокер отвечает
            client.describeCluster().nodes().get(3, TimeUnit.SECONDS);

            // 2) топик существует
            DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
            result.allTopicNames().get(3, TimeUnit.SECONDS);

            return Health.up()
                    .withDetail("topic", topic)
                    .withDetail("broker", "reachable")
                    .build();
        } catch (Exception e) {
            return Health.down(e)
                    .withDetail("topic", topic)
                    .build();
        }
    }
}
```

В `application-local.yml` можно явно:

```yaml
app:
  kafka:
    health-enabled: true
```

В `application-ci.yml`:

```yaml
app:
  kafka:
    health-enabled: false
```

---



## 10. Docker Compose — что добавить к MKAFKA‑01

**Зачем:** Postgres для таблицы заказов, pgAdmin чтобы глазами увидеть схему.

Фрагмент (добавь к существующим `kafka` / `akhq`; postgres у тебя уже может быть — синхронизируй пароль с `application-local.yml`):

```yaml
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: orders
      POSTGRES_USER: orders
      POSTGRES_PASSWORD: orders
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  pgadmin:
    image: dpage/pgadmin4:8
    depends_on:
      - postgres
    ports:
      - "5050:80"
    environment:
      PGADMIN_DEFAULT_EMAIL: mentor@company.ru
      PGADMIN_DEFAULT_PASSWORD: mentor
    volumes:
      - pgadmin_data:/var/lib/pgadmin

volumes:
  pgdata:
  pgadmin_data:
```

**Подключение в pgAdmin:**


| Поле                | Значение                           |
| ------------------- | ---------------------------------- |
| Host                | `postgres` (имя сервиса в compose) |
| Port                | `5432`                             |
| Database            | `orders`                           |
| Username / Password | `orders` / `orders`                |


---



## 11. OpenAPI и CI



### `docs/api/order-api.yaml`

Положи **точный** контракт из задания курса (title `mcloud-orders Order API`, paths `/api/v1/orders`, schemas).  
Servers URL лучше поправь на `http://localhost:8081`.

### `.github/workflows/ci.yml`

Как в задании: checkout → Java 21 → `./gradlew test` с `SPRING_PROFILES_ACTIVE=ci` → upload junit report.

Минимальный тест, чтобы CI не был пустым:

```java
package ru.mentee.power.orders;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("ci")
class McloudOrdersApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

---



## 12. Чеклист «создал ли я всё нужное?»

Отмечай галочками:

**Сборка**

- [x] `settings.gradle.kts` → имя `mcloud-orders`
- [x] `build.gradle.kts` → web, kafka, actuator, jpa, postgres, liquibase, test
- [x] `gradlew` работает: `./gradlew build`

**Конфиг**

- [x] `application.yml` (порт 8081, topic `order-events`, liquibase path)
- [x] `application-local.yml` (jdbc localhost)
- [x] `application-ci.yml` (H2, kafka health off)

**БД**

- [x] `changelog-master.yml` + SQL changeset
- [ ] в pgAdmin видна таблица `orders`

**Пакеты Java**

- [x] `McloudOrdersApplication`
- [x] domain: `Order`, `OrderLine`, `OrderStatus`, `PlaceOrderUseCase`
- [x] ports: `PlaceOrderPort`, `OrderEventPort`
- [x] web: controller, dto, mapper
- [x] kafka: `KafkaConfig`, `KafkaHealthIndicator`, `NoOpOrderEventAdapter`
- [x] persistence: `OrderEntity`, `OrderRepository`

**Прочее**

- [ ] `docs/api/order-api.yaml`
- [ ] `.github/workflows/ci.yml`
- [ ] pgAdmin в `docker-compose.yml`

---



## 13. Частые ошибки «куда положить файл»


| Ошибка                                                     | Почему плохо              | Как правильно                 |
| ---------------------------------------------------------- | ------------------------- | ----------------------------- |
| `KafkaTemplate` в `OrderController`                        | HTTP знает про брокер     | только через `OrderEventPort` |
| Domain‑класс с `@RestController`                           | мозг смешан с HTTP        | controller в `adapters.web`   |
| Таблицу создаёт Hibernate `update`                         | схема не в git            | только Liquibase              |
| Топик `orders.events`                                      | путаница имён             | `order-events`                |
| В yml `bootstrap-servers: kafka:29092` при запуске с хоста | имя `kafka` не резолвится | `localhost:9092`              |
| App на порту 8080                                          | конфликт с AKHQ           | `8081`                        |


---



## Связь с основным уроком


| Тема                               | Где читать                                               |
| ---------------------------------- | -------------------------------------------------------- |
| Зачем урок, карта, checkpoints     | [MKAFKA-02-каркас...](MKAFKA-02-каркас-mcloud-orders.md) |
| Как выглядят файлы (этот документ) | ты здесь                                                 |
| Kafka playground                   | [MKAFKA-01](MKAFKA-01-почему-kafka-и-playground.md)      |
| План курса                         | [../00-план-курса-kafka.md](../00-план-курса-kafka.md)   |


