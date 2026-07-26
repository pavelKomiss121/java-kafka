# MKAFKA-01 — Почему Kafka и playground

**Цель урока:** понять, зачем в m-CRM нужна Kafka, разобрать базовые понятия и поднять локальный стенд, с которым будем работать весь курс.

**Результат:** работающие Kafka + PostgreSQL + AKHQ в Docker; ты понимаешь, что лежит в `docker-compose.yml` и почему.

---

## 1. Аналогия: «тетрадь заказов»

Представь ресторан без Kafka:

- Официант кричит на кухню, на бар и в кассу одновременно.
- Если повар не услышал — заказ потерян.
- Если кухня перегружена — официант стоит и ждёт.

**С Kafka** появляется общая **тетрадь заказов** (топик):

1. Официант **пишет** строку: «Стол 5 — борщ, 12:03».
2. Кухня, бар и касса **читают** свою копию ленты в своём темпе.
3. Каждая строка **нумеруется** (offset) — можно продолжить с места остановки.
4. Несколько официантов → несколько **партиций** — параллельная запись без путаницы внутри одного стола.

В m-CRM то же самое:

| Роль в аналогии | Сервис | Событие |
|-----------------|--------|---------|
| Официант | `order-service` | `OrderCreated` |
| Кухня | `warehouse-service` | читает, резервирует товар |
| Касса | `billing-service` | выставляет счёт |
| SMS-уведомление | `notification-service` | шлёт клиенту статус |

Kafka — не замена REST. Это **шина событий**: «что произошло», а не «сделай X прямо сейчас».

---

## 2. Три жизненных кейса (зачем это бизнесу)

### Кейс 1 — Создание заказа

Клиент оформил заказ. Вместо 4 синхронных HTTP-вызовов:

```
order-service → HTTP → warehouse
              → HTTP → billing
              → HTTP → notification
              → HTTP → analytics
```

Один вызов + одно событие:

```
order-service → publish OrderCreated → topic order-events
   warehouse, billing, notification, analytics ← consume
```

**Выигрыш:** order-service отвечает клиенту быстро; остальные догоняют асинхронно.

### Кейс 2 — Смена статуса

Заказ перешёл `NEW → PAID → SHIPPED`. Каждый переход — отдельное событие в той же ленте. Подписчики видят **историю**, а не только текущее состояние.

**Выигрыш:** аудит, переигрывание событий, новый сервис может «дочитать» ленту с начала.

### Кейс 3 — Пик нагрузки (Чёрная пятница)

10 000 заказов в минуту. HTTP-сервисы падают от таймаутов. Kafka **буферизует** события: consumer'ы обрабатывают с той скоростью, которую выдерживают.

**Выигрыш:** система деградирует gracefully (растёт lag), а не падает каскадом.

---

## 3. Теория: минимум, без которого дальше нельзя

### Topic (топик)

Именованная лента сообщений. У нас главный топик курса — **`order-events`**.

Имена топиков: буквы, цифры, `-`, `_`. **Избегай точек** (`orders.events`) — путаница с метриками и ACL.

### Partition (партиция)

Топик делится на партиции для параллелизма. Сообщения с **одним ключом** (например `orderId`) попадают в одну партицию → порядок внутри заказа сохраняется.

```
topic order-events
├── partition 0: [evt1, evt4, evt7]
├── partition 1: [evt2, evt5]
└── partition 2: [evt3, evt6]
```

### Offset

Порядковый номер сообщения **внутри партиции**. Consumer запоминает offset → при рестарте продолжает с нужного места.

### Broker

Сервер Kafka. В нашем compose — один брокер в режиме **KRaft** (без ZooKeeper).

### Producer / Consumer / Consumer Group

| Роль | Делает |
|------|--------|
| **Producer** | Пишет в топик |
| **Consumer** | Читает из топика |
| **Consumer group** | Несколько consumer'ов делят партиции между собой |

---

## 4. Что мы сделали в репозитории

В этом репозитории (`java-kafka`) лежит **playground** — минимальная инфраструктура для урока MKAFKA-01. Java-кода пока нет: сначала поднимаем и понимаем окружение.

### Файлы

| Файл | Назначение |
|------|------------|
| `docker-compose.yml` | Kafka + PostgreSQL + AKHQ |
| `.env.example` | Шаблон `KAFKA_CLUSTER_ID` |
| `.env` | Твой локальный конфиг (в git не попадает) |
| `.gitignore` | Исключает `.env`, `target/`, IDE-файлы |

### Сервисы в `docker-compose.yml`

#### `kafka` (Confluent cp-kafka 7.7.1, KRaft)

Один узел совмещает **broker** и **controller** — достаточно для обучения.

Ключевые настройки:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_LISTENERS: PLAINTEXT://kafka:29092, ..., PLAINTEXT_HOST://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092, PLAINTEXT_HOST://localhost:9092
CLUSTER_ID: ${KAFKA_CLUSTER_ID:...}
```

**Два listener'а — зачем:**

| Listener | Кто подключается | Адрес |
|----------|------------------|-------|
| `PLAINTEXT` (`kafka:29092`) | Контейнеры в Docker-сети (AKHQ) | внутренний |
| `PLAINTEXT_HOST` (`localhost:9092`) | Твоё приложение на хосте (будущий Spring Boot) | снаружи |

> **Частая ошибка:** подключаться с хоста к `kafka:29092` или `localhost:29092`. С хоста нужен **`localhost:9092`**.

#### `postgres`

БД `orders` — пригодится с MKAFKA-02 (хранение заказов рядом с событийной лентой).

```
POSTGRES_DB: orders
POSTGRES_USER: orders
POSTGRES_PASSWORD: orders
Порт: 5432
```

#### `akhq`

Web-UI для Kafka: топики, сообщения, consumer groups, lag.

```
http://localhost:8080
```

Подключается к брокеру как `kafka:29092` (изнутри Docker-сети).

---

## 5. Практика: поднять playground

### Шаг 1 — Сгенерировать CLUSTER_ID

KRaft требует уникальный ID кластера **до первого запуска**:

```bash
docker run --rm confluentinc/cp-kafka:7.7.1 kafka-storage random-uuid
```

Скопируй UUID.

### Шаг 2 — Создать `.env`

```bash
cp .env.example .env
```

В `.env`:

```env
KAFKA_CLUSTER_ID=<твой-uuid>
```

### Шаг 3 — Запустить

```bash
docker compose up -d
docker compose ps
```

Все три сервиса должны быть `Up`. Если `kafka` в `Exited (1)` — см. раздел Troubleshooting.

### Шаг 4 — Проверить AKHQ

1. Открой http://localhost:8080
2. Connection `local-kafka` → Topics
3. Создай тестовый топик `order-events` (1 partition, replication factor 1)
4. Произведи несколько тестовых сообщений и убедись, что видишь их в UI

### Шаг 5 — (опционально) CLI с хоста

Если установлен `kafka-console-producer` локально:

```bash
# producer
kafka-console-producer --bootstrap-server localhost:9092 --topic order-events

# consumer (другой терминал)
kafka-console-consumer --bootstrap-server localhost:9092 --topic order-events --from-beginning
```

Без локального Kafka CLI достаточно AKHQ.

---

## 6. Troubleshooting

### Kafka падает с `Exited (1)`, в логах `CLUSTER_ID`

**Причина:** Confluent cp-kafka в KRaft-режиме читает переменную **`CLUSTER_ID`**, а не `KAFKA_CLUSTER_ID`.

**Было (не работало):**

```yaml
KAFKA_CLUSTER_ID: ${KAFKA_CLUSTER_ID}
```

**Стало (исправлено в нашем compose):**

```yaml
CLUSTER_ID: ${KAFKA_CLUSTER_ID:?generate with ...}
```

В `.env` по-прежнему `KAFKA_CLUSTER_ID=...` — compose пробрасывает значение в `CLUSTER_ID`.

### AKHQ: `Failed to create KafkaAdminClient`

Почти всегда Kafka ещё не поднялась или упала. Проверь:

```bash
docker compose logs kafka --tail 50
```

### Не могу подключиться с хоста

| Симптом | Решение |
|---------|---------|
| Connection refused на 9092 | `docker compose ps` — kafka Up? |
| Unknown host `kafka` | Ты на хосте — используй `localhost:9092` |
| Timeout на 29092 | 29092 — внутренний порт, с хоста не доступен |

### Порт 8080 занят

AKHQ и будущий Spring Boot оба хотят 8080. Варианты:

- остановить что занимает 8080;
- в MKAFKA-02 сдвинуть Spring на `8081` (или AKHQ на `8089`).

---

## 7. Связь с доменом m-CRM

Сервис **mcloud-orders** — центр спринта:

```
┌─────────────────┐     order-events      ┌──────────────────┐
│  order-service  │ ───────────────────►  │      Kafka       │
│  (REST API)     │                       │  topic: order-   │
│  + PostgreSQL   │                       │       events     │
└─────────────────┘                       └────────┬───────────┘
                                                 │
                    ┌────────────────────────────┼────────────────────────┐
                    ▼                            ▼                        ▼
            ┌──────────────┐            ┌──────────────┐         ┌──────────────┐
            │  warehouse   │            │   billing    │         │ notification │
            └──────────────┘            └──────────────┘         └──────────────┘
```

Сейчас у нас только нижний слой инфраструктуры (Kafka + Postgres). В MKAFKA-02 появится Spring Boot-сервис.

---

## 8. Самопроверка (8 вопросов)

Ответь себе письменно — без подглядывания в текст:

1. Чем событие в Kafka отличается от синхронного REST-вызова?
2. Что такое топик? Какой топик используем в курсе?
3. Зачем партиции? Что даёт ключ сообщения `orderId`?
4. Что такое offset и кто его хранит?
5. Почему в compose два listener'а (29092 и 9092)?
6. С какого адреса Spring Boot на хосте должен подключаться к Kafka?
7. Зачем нужен `CLUSTER_ID` в KRaft?
8. Что покажет AKHQ, если consumer отстаёт от producer'а?

---

## 9. Что дальше — MKAFKA-02

Следующий урок: [MKAFKA-02 — каркас mcloud-orders](MKAFKA-02-каркас-mcloud-orders.md)  
(+ [атлас файлов](MKAFKA-02-атлас-файлов.md)).

Там:

- каркас **Spring Boot** с **hexagonal architecture**;
- **Liquibase**-миграции для таблицы заказов;
- **pgAdmin** для просмотра PostgreSQL;
- **OpenAPI** + CI;
- порт приложения ≠ 8080 (чтобы не конфликтовать с AKHQ).

Инфраструктура из этого урока **остаётся** — к ней добавляется Java-код.

↑ [Спринт 01](README.md) · [План курса](../00-план-курса-kafka.md)

---

## Шпаргалка команд

```bash
# старт
docker compose up -d

# статус
docker compose ps

# логи kafka
docker compose logs kafka -f

# стоп
docker compose down

# стоп + удалить данные kafka/postgres
docker compose down -v
```
