# MKAFKA-02 — Каркас сервиса mcloud-orders

**Цель урока:** с нуля собрать Spring Boot‑сервис заказов, который **подключается** к Kafka и PostgreSQL из MKAFKA‑01, но ещё **не реализует** бизнес‑логику и публикацию событий (это MKAFKA‑03).

**Результат:** проект `mcloud-orders` собирается, поднимается с профилем `local`, отвечает на health‑чеки, имеет схему БД через Liquibase, заглушки REST по OpenAPI и правильную структуру пакетов (ports & adapters). В `docker-compose` есть PostgreSQL + pgAdmin.

**Важно про Kafka в этом уроке.**  
Ты **не учишь Kafka «в глубину» здесь** — ты учишь **окружение, в котором Kafka живёт**. Без REST, БД, миграций и health‑чеков Kafka на работе почти никогда не встречается «в одиночку». После этого урока у тебя будет «коробка», в которую в MKAFKA‑03 ты положишь producer/consumer.

Если MKAFKA‑01 ещё не пройден — сначала [MKAFKA-01](MKAFKA-01-почему-kafka-и-playground.md).

**Как создавать файлы (с примерами):** [MKAFKA-02 — атлас файлов](MKAFKA-02-атлас-файлов.md) — читай параллельно с этим уроком.

↑ [Спринт 01](README.md) · [План курса](../00-план-курса-kafka.md)

---

## 0. Карта: где Kafka, а где всё остальное

После MKAFKA‑01 у тебя есть «тетрадь» (брокер) и UI (AKHQ).  
После MKAFKA‑02 появляется **писатель тетради** — сервис `mcloud-orders`:

```
Клиент (curl / Postman)
        │  HTTP  POST /api/v1/orders
        ▼
┌───────────────────────────────────────────────┐
│              mcloud-orders (Spring Boot)       │
│  adapters.web  →  domain  →  ports.outgoing   │
│       │                │            │         │
│       │                ▼            │         │
│       │         PostgreSQL          │         │
│       │      (таблица orders)       │         │
│       │                             ▼         │
│       └──────────────────►  Kafka (пока TODO) │
└───────────────────────────────────────────────┘
        │                         │
        ▼                         ▼
   pgAdmin :5050            AKHQ :8080
```

| Слой | Что делает сейчас (MKAFKA‑02) | Когда заработает по‑настоящему |
|------|-------------------------------|--------------------------------|
| REST | Заглушки эндпоинтов по OpenAPI | MKAFKA‑03 (создание заказа) |
| Domain / UseCase | `UnsupportedOperationException` | MKAFKA‑03 |
| Kafka producer | Конфиг + health, публикация TODO | MKAFKA‑03 |
| PostgreSQL | Таблица через Liquibase | MKAFKA‑03 (сохранение) |
| Actuator | Health / liveness / readiness | уже в этом уроке |

**Аналогия.**  
MKAFKA‑01 — купили тетрадь и полку.  
MKAFKA‑02 — поставили стол, ручки, журнал учёта (БД), вывеску «открыто» (health).  
MKAFKA‑03 — начнём реально писать заказы в тетрадь и читать их.

---

## 1. Зачем этот урок, если мы изучаем Kafka?

На работе тебе почти всегда скажут: «подними сервис локально». В сервисе будет:

1. **HTTP API** — снаружи принимают запрос.
2. **База** — текущее состояние заказа (что ответить на GET).
3. **Kafka** — событие «заказ создан» для других сервисов.
4. **Health** — чтобы Kubernetes/Docker знали, жив ли сервис и доступен ли брокер.

Если умеешь только «поднять Kafka в Docker», а Spring‑проект собрать не можешь — на новом месте работы ты упрёшься в стену. Этот урок тренирует навык **развернуть современный Java‑проект с нуля**, не копируя готовый `build.gradle.kts`.

---

## 2. Повторение из MKAFKA‑01 (минимум для этого урока)

| Понятие | Одной фразой | Зачем в MKAFKA‑02 |
|---------|--------------|-------------------|
| **Брокер** | Сервер Kafka | Spring подключается к `localhost:9092` |
| **Топик** | Именованная лента сообщений | В конфиге указываем имя топика; health проверяет, что он есть |
| **Producer** | Кто пишет в топик | Появится в MKAFKA‑03; сейчас только порт `OrderEventPort` и конфиг |
| **Consumer** | Кто читает | Позже; пока не нужен |
| **Offset** | Номер сообщения в партиции | Пока не трогаем |

### Имя топика: важное исправление курса

В исходном задании иногда пишут `orders.events`.  
**В нашем курсе используем `order-events`** (через дефис).

Почему:

- точка в имени топика путается с метриками и внутренними именами Kafka;
- в памяти курса уже зафиксировано исправление: `order-events`, не `orders.events`.

Во всех `application*.yml`, OpenAPI‑заметках и README пиши **`order-events`**.

### Конфликт портов (обязательно учесть)

В MKAFKA‑01 **AKHQ** уже занимает **`8080`**.  
Spring Boot по умолчанию тоже хочет `8080` → конфликт.

**Решение:** приложение слушает **`8081`** (или сдвинь AKHQ на `8089` — хуже, потому что в задании OpenAPI показывает 8080; проще сдвинуть app и поправить URL в проверках).

В этом уроке везде ниже: приложение → `http://localhost:8081`.

---

## 3. Архитектура портов и адаптеров (простыми словами)

Это не «красота ради красоты». Это способ разложить код так, чтобы **Kafka и БД не протекли в бизнес‑логику**.

### Три зоны

```
                    ┌─────────────────────┐
   HTTP / Kafka /   │      adapters       │  ← «провода» к внешнему миру
   БД / UI          │  web, kafka, jpa    │
                    └──────────┬──────────┘
                               │ реализуют / вызывают
                    ┌──────────▼──────────┐
                    │       ports         │  ← «розетки» (интерфейсы)
                    │  incoming/outgoing  │
                    └──────────┬──────────┘
                               │ использует
                    ┌──────────▼──────────┐
                    │       domain        │  ← правила CRM: Order, UseCase
                    │  (не знает Kafka!)  │
                    └─────────────────────┘
```

| Пакет | Аналогия | Что внутри |
|-------|----------|------------|
| **domain** | Мозг | `Order`, `OrderStatus`, `PlaceOrderUseCase` — без Spring, без Kafka |
| **ports** | Контракты | «что умеем принимать» / «что умеем отдавать наружу» |
| **adapters** | Руки и глаза | REST‑контроллер, Kafka‑конфиг, JPA‑репозиторий |

### Incoming vs outgoing ports

- **Incoming** (`PlaceOrderPort`) — *вход*: «снаружи хотят оформить заказ». REST‑контроллер вызывает этот порт.
- **Outgoing** (`OrderEventPort`) — *выход*: «домену нужно опубликовать событие». UseCase вызывает порт; адаптер Kafka реализует его.

В MKAFKA‑02:

- интерфейсы портов — **есть**;
- use case — **заглушка** (`TODO` / `UnsupportedOperationException`);
- реализация Kafka‑адаптера (настоящий `send`) — **в MKAFKA‑03**.

Так ты не смешаешь «как устроен заказ» с «как сериализовать JSON в Kafka».

---

## 4. Целевая структура репозитория

Имя артефакта везде **`mcloud-orders`** (README, `spring.application.name`, скриншоты).

```
mcloud-orders/   (этот репозиторий java-kafka = практика курса)
├── build.gradle.kts
├── settings.gradle.kts          # rootProject.name = "mcloud-orders"
├── gradlew / gradlew.bat
├── docker-compose.yml
├── .env / .env.example
├── .gitignore
├── README.md
├── .github/workflows/ci.yml
├── docs/
│   ├── README.md
│   ├── 00-план-курса-kafka.md
│   ├── sprint-01-fundamentals/   ← уроки MKAFKA-01/02 (этот файл здесь)
│   ├── sprint-02-producer-consumer/
│   ├── sprint-03-production/
│   ├── api/order-api.yaml        ← OpenAPI (создаёшь в этом уроке)
│   └── architecture/             ← диаграмма пакетов
└── src/main/
    ├── java/ru/mentee/power/orders/
    │   ├── McloudOrdersApplication.java
    │   ├── domain/
    │   │   ├── model/Order.java
    │   │   ├── model/OrderLine.java
    │   │   ├── model/OrderStatus.java
    │   │   └── usecase/PlaceOrderUseCase.java
    │   ├── ports/
    │   │   ├── incoming/PlaceOrderPort.java
    │   │   └── outgoing/OrderEventPort.java
    │   └── adapters/
    │       ├── web/
    │       │   ├── OrderController.java
    │       │   ├── dto/OrderRequest.java
    │       │   ├── dto/OrderResponse.java
    │       │   └── mapper/OrderMapper.java
    │       ├── kafka/
    │       │   ├── KafkaConfig.java
    │       │   └── KafkaHealthIndicator.java
    │       └── persistence/OrderRepository.java
    └── resources/
        ├── application.yml
        ├── application-local.yml
        ├── application-test.yml
        ├── application-ci.yml
        └── liquibase/
            ├── changelog-master.yml
            └── changesets/
                └── 20250220-create-orders-table.sql
```

Базовый пакет: **`ru.mentee.power.orders`**.

### Не знаешь, какие файлы создавать?

Открой подробный разбор **каждого файла** (зачем нужен + пример содержимого + кто кого вызывает):

**→ [MKAFKA-02-атлас-файлов.md](MKAFKA-02-атлас-файлов.md)**

Там же: чеклист «создал ли всё», типичные ошибки раскладки пакетов, заглушки Java/YAML/Liquibase/Compose/CI.

---

## 5. План работы по шагам

Ниже — не «скопируй готовый проект», а **чеклист понимания**. Код набирай сам (Initializr / docs / AI — ок, но осознанно).

### Шаг 1. Инициализация Gradle + Spring Boot

Требования:

- **Java 21**
- **Gradle ≥ 8.5**
- **Spring Boot 3.2.x**

Варианты:

1. [start.spring.io](https://start.spring.io) → Dependencies: Web, Kafka, Actuator, Data JPA, PostgreSQL, Liquibase → скачать ZIP → разложить в репозиторий.
2. Или `gradle init` + руками плагины в `build.gradle.kts`.

Удали демо‑классы (`DemoApplication` и т.п.). Точка входа:

```text
ru.mentee.power.orders.McloudOrdersApplication
```

`settings.gradle.kts`:

```kotlin
rootProject.name = "mcloud-orders"
```

Проверка: `./gradlew build` проходит.

### Шаг 2. Зависимости (что зачем — связь с Kafka)

В `build.gradle.kts` должны появиться (смысл важнее синтаксиса):

| Зависимость | Зачем |
|-------------|--------|
| `spring-boot-starter-web` | REST API `/api/v1/orders` |
| `spring-kafka` | Клиент Kafka (Template, AdminClient, конфиг) |
| `spring-boot-starter-actuator` | `/actuator/health`, в т.ч. Kafka health |
| `spring-boot-starter-data-jpa` | Репозиторий заказов (состояние в БД) |
| `postgresql` (42.7.x) | Драйвер к Postgres |
| `liquibase-core` | Миграции схемы (таблица `orders`) |
| `spring-boot-starter-test` | Юнит/интеграционные тесты |
| `spring-kafka-test` | Тестовые утилиты Kafka (профиль `test`/`ci`) |

Плагины: `org.springframework.boot` 3.2.x, `io.spring.dependency-management`, `java`.

> Liquibase через Gradle‑таск `liquibaseUpdate` потребует либо плагин Liquibase, либо запуск миграций при старте Spring (`spring.liquibase.enabled=true`). Для курса достаточно автозапуска при `bootRun` + ручная проверка в pgAdmin. Если оставляешь `./gradlew liquibaseUpdate` — добавь liquibase‑gradle‑plugin и datasource в его конфиг (опиши команды в README).

### Шаг 3. `application.yml` — «общие правила»

Зафиксируй:

- `spring.application.name=mcloud-orders`
- Kafka: `bootstrap-servers: localhost:9092`, топик **`order-events`**
- Actuator: expose `health,info`; `show-details=always`; `health.probes.enabled=true`
- JPA: `hibernate.ddl-auto=validate` (схему меняет **только Liquibase**, не Hibernate)
- `spring.liquibase.change-log=classpath:liquibase/changelog-master.yml`
- `server.port=8081` (из‑за AKHQ на 8080)

Пример смысловой структуры (набери сам, значения подгони):

```yaml
spring:
  application:
    name: mcloud-orders
  kafka:
    bootstrap-servers: localhost:9092
    # свойство имени топика можно вынести в app.kafka.topic
  liquibase:
    change-log: classpath:liquibase/changelog-master.yml
    enabled: true
  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: true

server:
  port: 8081

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
    team: mentee-power
    # repository: ссылка на твой git remote

app:
  kafka:
    topic: order-events
```

### Шаг 4. Профили: `local`, `test`, `ci`

| Профиль | Когда | Kafka | БД |
|---------|-------|-------|-----|
| **local** | Твой ноутбук | `localhost:9092` (Docker) | Postgres `localhost:5432` |
| **test** | Автотесты | заглушка / testcontainers / mock | H2 или testcontainers |
| **ci** | GitHub Actions | **не требует** живого брокера | in‑memory (H2) или отключённая интеграция |

Запуск локально:

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

В `application-local.yml` — URL/user/password Postgres, при необходимости переопределение Kafka.

В `application-ci.yml` — H2, `spring.autoconfigure.exclude` для Kafka **или** test‑заглушки, чтобы `./gradlew test` на CI не ждал брокер.

### Шаг 5. Docker Compose: Postgres + pgAdmin

В MKAFKA‑01 у тебя уже могут быть `kafka`, `postgres`, `akhq`.  
В MKAFKA‑02 **обязательно** добавить/привести:

**PostgreSQL**

| Параметр | Рекомендация курса |
|----------|--------------------|
| image | `postgres:16-alpine` |
| port | `5432` |
| DB / user | `orders` / `orders` |
| password | в задании часто `secret`; в текущем compose может быть `orders` — **выбери одно и синхронизируй с `application-local.yml`** |

**pgAdmin**

| Параметр | Значение |
|----------|----------|
| port | `5050` |
| email | `mentor@company.ru` |
| password | любой свой |

После `docker compose up -d`:

1. Открой http://localhost:5050  
2. Add New Server → Host = `postgres` (изнутри Docker) или `host.docker.internal` / `localhost` (если pgAdmin смотрит на хост — зависит от сети compose).  
   Если pgAdmin в том же `docker-compose`, host сервера БД = **имя сервиса** `postgres`.  
3. Сохрани подключение — это чекпоинт урока.

### Шаг 6. Liquibase: фиксируем схему

Файлы:

1. `liquibase/changelog-master.yml` — подключает changeset  
2. `liquibase/changesets/20250220-create-orders-table.sql` — formatted SQL

Master:

```yaml
databaseChangeLog:
  - include:
      file: liquibase/changesets/20250220-create-orders-table.sql
```

Changeset (идея, не копипаста «вслепую»):

```sql
--liquibase formatted sql

--changeset mentee:20250220-create-orders
CREATE TABLE orders (
    id          UUID PRIMARY KEY,
    customer_id UUID,
    amount      NUMERIC(19, 2) NOT NULL,
    status      VARCHAR(32) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_orders_status ON orders (status);
CREATE INDEX idx_orders_created_at ON orders (created_at);

--rollback DROP TABLE orders;
```

Почему `ddl-auto=validate`: Hibernate **проверяет**, что сущности совпадают со схемой, но **не создаёт** таблицы. Источник правды — Liquibase.

Проверка: таблица `orders` видна в pgAdmin.

### Шаг 7. Каркас пакетов (только заглушки)

Создай классы **пустыми/с TODO**, без настоящей логики.

| Класс | Роль | Что внутри в MKAFKA‑02 |
|-------|------|-------------------------|
| `McloudOrdersApplication` | `@SpringBootApplication` | пустой main |
| `PlaceOrderPort` | incoming port | метод вроде `place(...)` |
| `OrderEventPort` | outgoing port | метод `publish(...)` |
| `PlaceOrderUseCase` | implements PlaceOrderPort | `throw new UnsupportedOperationException("TODO MKAFKA-03")` |
| `Order`, `OrderLine`, `OrderStatus` | domain model | поля по смыслу OpenAPI (id, amount, status, priority…) |
| `OrderController` | REST | методы create/get → вызывают port; пока 501 или делегируют в use case‑заглушку |
| `OrderRequest` / `OrderResponse` | DTO | поля строго по OpenAPI |
| `OrderMapper` | domain ↔ DTO | методы‑заглушки |
| `KafkaConfig` | `@Configuration` | `NewTopic` для `order-events`, сериализаторы String/JSON |
| `KafkaHealthIndicator` | HealthIndicator | брокер доступен + топик существует (`AdminClient`) |
| `OrderRepository` | Spring Data | интерфейс `JpaRepository<...>` (сущность JPA можно добавить минимально или пометить TODO) |

**Не перескакивай на реализацию PlaceOrder** — цель урока площадка, не бизнес.

### Шаг 8. OpenAPI контракт

Положи точный контракт в:

`docs/api/order-api.yaml`

Эндпоинты:

| Метод | Путь | Ответ |
|-------|------|--------|
| POST | `/api/v1/orders` | **202** + `OrderResponse` |
| GET | `/api/v1/orders/{orderId}` | **200** или **404** |

Схемы: `OrderRequest` (customerId, amount, lines), `OrderLine` (productId, quantity, price, priority), `OrderResponse` (orderId, status, amount, createdAt).

Статусы: `NEW`, `PROCESSING`, `PAID`, `SHIPPED`, `CANCELLED`.  
Приоритеты линий: `HIGH`, `NORMAL`, `LOW`.

В README поправь URL на порт **8081**, если так решил.

### Шаг 9. Health и Kafka

Нужны:

- `GET /actuator/health` → в идеале `UP` (если брокер поднят)
- `GET /actuator/health/liveness`
- `GET /actuator/health/readiness`
- кастомный `KafkaHealthIndicator`:  
  1) можно ли достучаться до bootstrap‑servers;  
  2) существует ли топик `order-events` (через `AdminClient.describeTopics` / listTopics).

`KafkaConfig` может объявить `NewTopic` — тогда при старте Spring **создаст топик**, если его ещё нет (проверишь в AKHQ).

### Шаг 10. CI (GitHub Actions)

Файл `.github/workflows/ci.yml` — как в задании курса:

- Java 21 Temurin  
- `SPRING_PROFILES_ACTIVE=ci`  
- `./gradlew test`  
- upload artifact с отчётом тестов  

Профиль `ci` **не должен** требовать Docker Kafka/Postgres на раннере.

---

## 6. Как это стыкуется с Kafka (если пока «ничего не понимаю»)

Разложи один будущий запрос (MKAFKA‑03) на шаги:

1. Клиент шлёт `POST /api/v1/orders` с JSON.  
2. `OrderController` (adapter web) принимает DTO.  
3. `OrderMapper` переводит DTO → domain.  
4. `PlaceOrderUseCase` (domain) проверяет правила, создаёт `Order`.  
5. Сохранение в Postgres через persistence‑adapter (состояние для GET).  
6. `OrderEventPort.publish(...)` — **здесь появится запись в Kafka**.  
7. Адаптер Kafka сериализует событие и делает `KafkaTemplate.send("order-events", key, payload)`.  
8. Склад/биллинг (другие сервисы) **читают** тот же топик — их в этом курсе пока нет, но тетрадь уже общая.

Сейчас (урок 02) шаги 5–7 — **заглушки и конфиг**.  
Ты готовишь «розетку» `OrderEventPort` и провод `KafkaConfig`, но вилку ещё не вставляешь.

**Почему health Kafka важен уже сейчас:**  
если брокер упал, readiness может стать DOWN — оркестратор не будет слать трафик на мёртвый инстанс. Для локалки ты видишь проблему раньше, чем начнёшь дебажить producer.

---

## 7. Checkpoints (отмечай в README)

| # | Проверка | Ожидание |
|---|----------|----------|
| 1 | `./gradlew build` | BUILD SUCCESSFUL |
| 2 | `docker compose up -d` | kafka, postgres, akhq, pgadmin Up |
| 3 | Liquibase (старт app или `liquibaseUpdate`) | таблица `orders` в pgAdmin |
| 4 | `./gradlew bootRun --args='--spring.profiles.active=local'` | `Started McloudOrdersApplication` (не `McloudKafkaApplication` — опечатка в старом задании) |
| 5 | `curl http://localhost:8081/actuator/health` | `"status":"UP"` |
| 6 | liveness / readiness | UP |
| 7 | AKHQ | топик `order-events` виден |
| 8 | CI‑файл в репо | есть `.github/workflows/ci.yml` + `application-ci.yml` |
| 9 | OpenAPI | `docs/api/order-api.yaml` |

Пример проверок:

```bash
curl http://localhost:8081/actuator/health
curl http://localhost:8081/actuator/health/liveness
curl http://localhost:8081/actuator/health/readiness
curl http://localhost:8081/actuator/info
```

---

## 8. Самостоятельное задание (из курса)

1. Профили `local` и `test` (+ `ci` для Actions).  
2. Таблица в README: Endpoint → Описание → команда проверки.  
3. Диаграмма зависимостей пакетов → `docs/architecture/`.  
4. Заметка по логированию: какие поля заказа/события логировать всегда (`orderId`, `status`, `topic`, `partition`, `offset` — когда появится producer).

---

## 9. Troubleshooting

| Симптом | Причина | Что сделать |
|---------|---------|-------------|
| Port 8080 already in use | AKHQ занимает 8080 | `server.port=8081` |
| `Connection refused:9092` | Kafka не поднята / VT-x выключен | MKAFKA‑01 + BIOS virtualization + Docker Desktop |
| Health DOWN / kafka | Нет брокера или неверный bootstrap | `localhost:9092` с хоста, не `kafka:29092` |
| Liquibase / ddl validate fail | Таблица не создана или поля разъехались | проверь changeset и entity |
| CI падает на Kafka | Профиль `ci` ходит на реальный брокер | отключи автоконфиг Kafka или используй H2 + моки |
| Топик `orders.events` | Старое имя из задания | переименуй на `order-events` |
| Postgres auth failed | Пароль в compose ≠ yml | синхронизируй `orders`/`secret` |

---

## 10. Самопроверка (12 вопросов)

1. Чем domain отличается от adapters?  
2. Зачем нужен `OrderEventPort`, если Kafka ещё не пишем?  
3. Почему `PlaceOrderUseCase` не должен импортировать `KafkaTemplate`?  
4. Чем incoming port отличается от outgoing?  
5. Почему `ddl-auto=validate`, а не `update`?  
6. Что проверяет `KafkaHealthIndicator`?  
7. С какого адреса Spring на хосте стучится в Kafka?  
8. Почему приложение не на порту 8080 рядом с AKHQ?  
9. Зачем профиль `ci` отдельный от `local`?  
10. Какое имя топика используем в курсе и почему не с точкой?  
11. Что из OpenAPI обязано совпасть с DTO один‑в‑один?  
12. Что останется TODO до MKAFKA‑03?

---

## 11. Что дальше — MKAFKA‑03

В следующем уроке:

- реализация `PlaceOrderUseCase` (создание заказа, статус `NEW`);
- сохранение в БД;
- публикация события в `order-events` через `OrderEventPort` + `KafkaTemplate`;
- разбор ключа сообщения (`orderId`), сериализации и проверки в AKHQ.

Каркас из этого урока **не переписывается** — только наполняется.

---

## Шпаргалка команд

```bash
# инфраструктура
docker compose up -d
docker compose ps

# приложение
./gradlew build
./gradlew bootRun --args='--spring.profiles.active=local'

# проверки
curl http://localhost:8081/actuator/health
curl http://localhost:8081/actuator/info

# UI
# AKHQ:    http://localhost:8080
# pgAdmin: http://localhost:5050
```

---

## Связанные файлы курса в этом репо

| Файл | Зачем |
|------|--------|
| [../00-план-курса-kafka.md](../00-план-курса-kafka.md) | карта спринтов |
| [MKAFKA-01-...](MKAFKA-01-почему-kafka-и-playground.md) | брокер, топик, playground |
| [MKAFKA-02-атлас-файлов.md](MKAFKA-02-атлас-файлов.md) | примеры всех файлов каркаса |
| [../api/](../api/README.md) | сюда — OpenAPI `order-api.yaml` |
| [../architecture/](../architecture/README.md) | сюда — диаграммы |
