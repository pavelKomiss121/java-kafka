# MKAFKA-03 — Producer: REST → Domain → Kafka

**Цель урока:** превратить заглушки из MKAFKA‑02 в **работающий продюсер**: REST принимает заказ из CRM, домен валидирует и собирает модель, Kafka‑адаптер публикует событие в топик по **приоритету**, ключ сообщения = **region**. Плюс метрики отправки и проверка в AKHQ / curl / unit‑тестах.

**Результат:** `POST /api/v1/orders` → `202` + `orderId`; в AKHQ видны сообщения в `orders.priority.high|normal|low`; `GET /api/v1/orders/metrics` отдаёт счётчики; `PlaceOrderUseCaseTest` зелёный.

**Что ещё НЕ делаем (MKAFKA‑04):** consumer и сохранение подтверждений в PostgreSQL. В БД сейчас можно держать заказ **в памяти** (Map) — persistence через JPA оставь `TODO`.

Если каркас не готов — сначала [MKAFKA-02](../sprint-01-fundamentals/MKAFKA-02-каркас-mcloud-orders.md) и [атлас](../sprint-01-fundamentals/MKAFKA-02-атлас-файлов.md).

**Атлас файлов этого урока:** [MKAFKA-03-атлас-файлов.md](MKAFKA-03-атлас-файлов.md)

> Код урока **уже реализован** в `src/main/java/...`. Читай теорию ниже, сверяй с файлами в IDE.

↑ [Спринт 02](README.md) · [План курса](../00-план-курса-kafka.md)

---

## Азбука архитектуры (если ничего не понятно — начни здесь)

Этот раздел специально «на пальцах». Без него названия `port`, `adapter`, `use case`, `record` кажутся магией.

### Зачем вообще делить код на папки?

Представь ресторан:

| Роль в ресторане | В коде | Папка |
|------------------|--------|--------|
| Официант принимает заказ у гостя | HTTP / JSON | `adapters/web` |
| Меню и правила кухни («нельзя борщ без тарелки») | бизнес | `domain` |
| Договорённость: «кухня умеет принять заказ» | контракт | `ports` |
| Повар пишет в общую тетрадь для склада | Kafka | `adapters/kafka` |

Если официант сам бежит на склад и сам варит суп — хаос: сменить «тетрадь» (Kafka → Rabbit) придётся переписывать всё.  
Поэтому: **официант → договор → кухня → договор → складская тетрадь**.

### Что такое domain (домен)?

**Domain** — «смысл бизнеса», без технологий.

- `Order` — заказ (id, сумма, регион, приоритет…)
- `OrderPriority` — HIGH / NORMAL / LOW
- Правила: «сумма > 0», «нужна хотя бы одна позиция»

Domain **не знает**, что такое HTTP, JSON, PostgreSQL, Kafka.  
Ему всё равно, заказ пришёл из Postman или из мобилки.

### Что такое use case?

**Use case (сценарий использования)** — один конкретный сценарий бизнеса.

Пример: *«Оформить заказ»* → класс `PlaceOrderUseCase`.

Он делает по шагам:

1. проверить данные;
2. создать `Order`;
3. запомнить (пока в памяти);
4. попросить «опубликовать событие» через порт;
5. вернуть результат `QUEUED`.

Это не контроллер (тот про HTTP) и не Kafka (та про брокер).  
Use case = **режиссёр сцены**.

### Что такое port (порт)?

**Port** — это **интерфейс-договор**, розетка.

- **Incoming port** (`PlaceOrderPort`) — «снаружи можно попросить оформить заказ».  
  Его вызывает web-адаптер. Его реализует use case.
- **Outgoing port** (`OrderEventPort`) — «домену нужно уметь опубликовать событие».  
  Его вызывает use case. Его реализует Kafka-адаптер.

Почему интерфейс, а не сразу класс?

```text
PlaceOrderUseCase ──вызывает──► OrderEventPort   (интерфейс)
                                      ▲
                                      │ реализует
                               OrderEventProducer (Kafka)
```

Завтра можно подставить другую реализацию (тест-мок, Rabbit, файл) — **use case не меняется**.

Аналогия: розетка 220В. Тебе всё равно, лампа это или зарядка — вилка подходит.

### Что такое adapter (адаптер)?

**Adapter** — «вилка» к внешнему миру, реализация порта или вход снаружи.

| Адаптер | С чем говорит | Пример |
|---------|---------------|--------|
| `adapters.web` | браузер / CRM по HTTP | `OrderController` |
| `adapters.kafka` | брокер Kafka | `OrderEventProducer` |
| `adapters.persistence` | PostgreSQL | `OrderRepository` (пока почти не трогаем) |

Адаптер **переводит** чужой язык в язык домена:

- JSON → `PlaceOrderCommand`
- `OrderEventPayload` → байты в топик

### Картинка одним взглядом

```
     Postman / CRM
           │  JSON
           ▼
   ┌───────────────────┐
   │  OrderController  │  ← adapter (web)
   │  OrderMapper      │
   └─────────┬─────────┘
             │ PlaceOrderCommand
             ▼
   ┌───────────────────┐
   │  PlaceOrderPort   │  ← incoming port (интерфейс)
   └─────────┬─────────┘
             │ реализует
             ▼
   ┌───────────────────┐
   │ PlaceOrderUseCase │  ← use case (domain)
   └─────────┬─────────┘
             │ OrderEventPayload
             ▼
   ┌───────────────────┐
   │  OrderEventPort   │  ← outgoing port (интерфейс)
   └─────────┬─────────┘
             │ реализует
             ▼
   ┌───────────────────┐
   │OrderEventProducer │  ← adapter (kafka)
   └─────────┬─────────┘
             ▼
          Kafka topics
```

### Что такое record?

`record` в Java 16+ — **короткий неизменяемый класс «только данные»**.

Вместо:

```java
public class PlaceOrderResult {
    private final UUID orderId;
    private final String status;
    // конструктор, геттеры, equals, hashCode, toString...
}
```

пишешь:

```java
public record PlaceOrderResult(UUID orderId, String status, Instant dispatchedAt) {}
```

Компилятор сам сделает:

- поля `final`;
- конструктор;
- методы `orderId()`, `status()`, `dispatchedAt()` (не `getOrderId`!);
- `equals` / `hashCode` / `toString`.

**Когда record уместен:** команда, результат, событие Kafka, DTO «перенести данные».  
**Когда обычный class:** сущность с изменяемым состоянием и кучей поведения (иногда `Order` оставляют class).

В нашем коде records:

- `PlaceOrderCommand` / `PlaceOrderResult` — вход/выход use case;
- `OrderEventPayload` — что улетает в Kafka.

### Command vs DTO — зачем два?

| | `OrderRequest` (DTO) | `PlaceOrderCommand` |
|--|----------------------|---------------------|
| Откуда | JSON с HTTP | уже «наш» объект после mapper |
| Где живёт | `adapters.web.dto` | `ports.incoming` |
| Знает ли domain | нет | да (через port) |
| Типы | `String priority` | `OrderPriority priority` enum |

Mapper переводит «грязный» JSON в чистую команду. Domain не парсит строки `"HIGH"`.

### Producer, топик, ключ — ещё раз по-русски

1. **Producer** — кто **пишет** в Kafka (наш сервис).  
2. **Topic** — именованная лента. У срочных заказов своя лента `orders.priority.high`.  
3. **Key** (`region`) — бирка на сообщении. Одинаковый ключ → одна партиция → порядок внутри региона.  
4. **Value** — тело события (`OrderEventPayload` в JSON).  
5. **Callback** — «отправилось / упало» → крутим счётчики metrics.

---

## 0. Зачем продюсер в m-CRM

В CRM менеджер жмёт «Создать заказ». Синхронно нужно быстро ответить: «принято».  
Дальше склад, уведомления, платежи — **асинхронно**, через Kafka.

```
CRM / Postman
    │  POST /api/v1/orders
    ▼
 OrderController  →  PlaceOrderPort  →  PlaceOrderUseCase
                                              │
                              ┌───────────────┴───────────────┐
                              ▼                               ▼
                     (память / TODO БД)              OrderEventPort
                                                              │
                                                              ▼
                                                    OrderEventProducer
                                                              │
                    ┌─────────────────┬───────────────────────┴────────┐
                    ▼                 ▼                                 ▼
         orders.priority.high   orders.priority.normal      orders.priority.low
                    (ключ сообщения = region, напр. EU)
```

**Аналогия к MKAFKA‑01.**  
Тетрадь больше не одна: для срочных заказов — красная, для обычных — синяя, для низких — зелёная.  
Регион (`EU`, `RU`) — «столбец»: сообщения одного региона в одной партиции → порядок внутри региона сохраняется.

---

## 1. Повторение Kafka (только то, что нужно продюсеру)

| Понятие | В этом уроке |
|---------|----------------|
| **Producer** | `OrderEventProducer` через `KafkaTemplate` |
| **Topic** | Три топика по приоритету (см. ниже) |
| **Key** | `region` (строка) |
| **Value** | JSON payload события (`OrderEventPayload`) |
| **Callback** | успех / ошибка → метрики + логи |
| **Idempotence** | `enable.idempotence=true` — меньше дублей при ретраях брокера |

### Имена топиков (задание MKAFKA‑03)

В задании явно:

| Priority | Топик |
|----------|--------|
| `HIGH` | `orders.priority.high` |
| `NORMAL` | `orders.priority.normal` |
| `LOW` | `orders.priority.low` |

> Раньше в курсе для одного топика рекомендовали `order-events` (без точки).  
> Здесь **следуй заданию урока 3**: топики с `orders.priority.*`.  
> Точки в имени — ок, если так требует контракт команды; главное — единообразие в yml / resolver / AKHQ.

### Порт приложения

У нас AKHQ на **8080**, Spring — на **8081** (из MKAFKA‑02).  
В OpenAPI задания часто пишут `:8080` — в curl/README используй **`http://localhost:8081`**.

---

## 2. Поток через ports & adapters (детально)

```
1. HTTP JSON OrderRequest
2. OrderController        — валидация транспорта, HTTP-коды
3. OrderMapper            — DTO → PlaceOrderCommand (не domain JSON!)
4. PlaceOrderPort.submit / place(...)
5. PlaceOrderUseCase      — бизнес-правила, Order, вызов исходящего порта
6. OrderEventPort.publish(...)
7. OrderEventProducer     — resolveTopic(priority), ProducerRecord(topic, region, payload)
8. KafkaTemplate.send + callback → ProducerMetricsRegistry
9. Ответ 202 OrderAcceptedResponse { orderId, status: QUEUED, dispatchedAt }
```

**Правила границ:**

| Слой | Можно | Нельзя |
|------|--------|--------|
| `adapters.web` | HTTP, DTO, mapper, вызов incoming port | `KafkaTemplate` |
| `domain.usecase` | правила, создание `Order`, вызов `OrderEventPort` | знать имена топиков / serializers |
| `adapters.kafka` | `KafkaTemplate`, топики, callback | бизнес-валидация суммы/линий |
| `ports.*` | только интерфейсы и command/result | Spring Web / Kafka API |

Теория в конспекте с `Invoice*` / `SubmitOrderPort` — **иллюстрация приёма**.  
В нашем проекте имена: **`PlaceOrderPort`**, **`OrderEventPort`**, **`OrderEventProducer`**, payload заказа.

---

## 3. Что меняется относительно MKAFKA‑02

| Было (02) | Станет (03) |
|-----------|-------------|
| `PlaceOrderUseCase` → `UnsupportedOperationException` | реальная валидация + публикация |
| `NoOpOrderEventAdapter` | `OrderEventProducer` (или переименуй/замени no-op) |
| Один топик `order-events` в конфиге | три топика + `KafkaTopicResolver` |
| Нет метрик | `ProducerMetricsRegistry` + `GET .../metrics` |
| `Order` без region/priority | поля `region`, `priority` (+ lines) |
| OpenAPI 0.1.0 | 0.2.0: `202`, `OrderAcceptedResponse`, metrics, errors |

Сохранение в PostgreSQL через `OrderRepository` — **TODO MKAFKA‑04**. Сейчас достаточно `ConcurrentHashMap<UUID, Order>` в use case или отдельном in-memory adapter.

---

## 4. Контракты (OpenAPI → код)

### POST `/api/v1/orders`

**Request** обязательные поля: `customerId`, `region`, `priority`, `amount`, `lines` (≥1).

**Валидация (отклонять 400):**

- нет позиций / пустой `lines`
- `amount` ≤ 0
- пустой `region`
- (логично) `quantity` < 1, неизвестный `priority`

**Response 202:**

```json
{
  "orderId": "...",
  "status": "QUEUED",
  "dispatchedAt": "2026-07-23T12:00:00Z"
}
```

**500** — не удалось отправить в Kafka (future завершился ошибкой).

### GET `/api/v1/orders/metrics`

```json
{
  "totals": { "success": 3, "failure": 0 },
  "topics": {
    "orders.priority.high": { "success": 2, "failure": 0 },
    "orders.priority.normal": { "success": 1, "failure": 0 }
  }
}
```

Полный yaml: обнови `docs/api/order-api.yaml` (версия API 0.2.0).  
Пример ответа metrics → `docs/examples/`.

---

## 5. План работы по шагам

1. **Расширь domain** — `Order` / priority enum / region.  
2. **Перепиши ports** — `PlaceOrderCommand` + result; `OrderEventPort.publish(payload, priority)` → `CompletionStage<Void>` (или `CompletableFuture`).  
3. **Реализуй use case** — validate → create Order → (memory) → publish → return accepted.  
4. **KafkaTopicResolver** — priority → topic name из конфига.  
5. **OrderEventProducer** — `KafkaTemplate<String, OrderEventPayload>`, key=region, callbacks → metrics.  
6. **KafkaProducerConfig** — serializers JSON, idempotence, `NewTopic` для трёх топиков.  
7. **ProducerMetricsRegistry** — success/failure totals + per topic.  
8. **OrderController** — POST + GET metrics, коды 202/400/500.  
9. **application-local.yml** — имена топиков, idempotence.  
10. **Unit-тест** `PlaceOrderUseCaseTest` с моком `OrderEventPort`.  
11. **OpenAPI + README + скрин AKHQ**.

Детали кода — в [атласе](MKAFKA-03-атлас-файлов.md).

---

## 6. Теория продюсера (чуть глубже)

### ProducerRecord

```text
ProducerRecord(topic, key, value)
```

- **topic** — куда писать (у нас от priority)  
- **key** — `region` → одинаковый ключ → одна партиция → порядок внутри региона  
- **value** — payload события

### send + callback

`KafkaTemplate.send(...)` возвращает future. В callback:

- **success** → increment success для топика, лог partition/offset  
- **failure** → increment failure, проброс ошибки use case / 500

В Spring Kafka 3.x часто используют `CompletableFuture` (`send().whenComplete(...)`), а не устаревший `ListenableFuture.addCallback` из примеров конспекта — идея та же.

### Idempotence

```yaml
spring.kafka.producer.properties.enable.idempotence: true
```

Брокер и продюсер договариваются не плодить дубли при ретраях сети. Это **не** замена идемпотентности на стороне consumer (MKAFKA‑04+).

### Метрики «свои» vs Actuator

В уроке — учебный REST `/metrics`. В бою чаще Micrometer + `/actuator/prometheus`. Сейчас цель — понять счётчики по топикам руками.

---

## 7. Checkpoints

| # | Проверка | Ожидание |
|---|----------|----------|
| 1 | `./gradlew build` | SUCCESS |
| 2 | `bootRun --spring.profiles.active=local` | Started… |
| 3 | `POST .../api/v1/orders` (HIGH) | 202 + orderId |
| 4 | AKHQ | сообщение в `orders.priority.high`, key=`EU` |
| 5 | NORMAL / LOW | другие топики |
| 6 | плохой amount / empty lines | 400 |
| 7 | `GET .../metrics` | success растёт |
| 8 | `./gradlew test --tests ...PlaceOrderUseCaseTest` | green |
| 9 | OpenAPI 0.2.0 в `docs/api/` | обновлён |
| 10 | README «Проверка отправки» | curl + шаги AKHQ |

---

## 8. Команды проверки

```bash
docker compose up -d

./gradlew bootRun --args='--spring.profiles.active=local'

curl -X POST http://localhost:8081/api/v1/orders \
  -H "Content-Type: application/json" \
  -d "{
        \"customerId\": \"550e8400-e29b-41d4-a716-446655440000\",
        \"priority\": \"HIGH\",
        \"region\": \"EU\",
        \"amount\": 1200.50,
        \"lines\": [
          {\"productId\": \"6ba7b814-9dad-11d1-80b4-00c04fd430c8\", \"quantity\": 2, \"price\": 600.25}
        ]
      }"

curl http://localhost:8081/api/v1/orders/metrics

./gradlew test --tests "ru.mentee.power.orders.domain.usecase.PlaceOrderUseCaseTest"
```

В AKHQ: Topics → `orders.priority.high` → Data → ключ `EU`, JSON value.

Скриншоты → `docs/screenshots/` (три топика + одно сообщение).

---

## 9. Troubleshooting

| Симптом | Что проверить |
|---------|----------------|
| 202 есть, в AKHQ пусто | bootstrap `localhost:9092`, топик создан, профиль `local` |
| Всё в одном топике | `KafkaTopicResolver` / priority в payload |
| Ключ null | в `ProducerRecord` вторым аргументом передай `region` |
| 500 на publish | логи callback; брокер Up? |
| Тест падает на Kafka | мокай `OrderEventPort`, не поднимай брокер |
| Порт занят | приложение **8081**, не 8080 |

---

## 10. Самопроверка

1. Почему контроллер не должен вызывать `KafkaTemplate`?  
2. Зачем `PlaceOrderCommand` отдельно от `OrderRequest`?  
3. Что даёт ключ = `region`?  
4. Чем HIGH‑топик отличается от LOW на уровне Kafka?  
5. Что считает metrics при ошибке `send`?  
6. Зачем idempotence на продюсере?  
7. Почему сохранение в Postgres — TODO, а событие уже шлём?  
8. Чем `CompletionStage` на порту лучше `void` + глотание ошибок?

---

## 11. Что дальше — MKAFKA‑04

Consumer читает те же топики (или общий поток), подтверждает заказ, пишет в PostgreSQL.  
Продюсер из этого урока **не переписывается** — только добавляется consumer‑адаптер.

---

## Связанные материалы

| Файл | Зачем |
|------|--------|
| [MKAFKA-03-атлас-файлов.md](MKAFKA-03-атлас-файлов.md) | вид каждого файла |
| [MKAFKA-02](../sprint-01-fundamentals/MKAFKA-02-каркас-mcloud-orders.md) | каркас |
| [MKAFKA-01](../sprint-01-fundamentals/MKAFKA-01-почему-kafka-и-playground.md) | брокер / AKHQ |
| `docs/api/order-api.yaml` | контракт 0.2.0 |
