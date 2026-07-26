# План курса Kafka для Java-разработчика

> Этот файл — «карта» курса.  
> Оглавление папок: [docs/README.md](README.md)

**Спринт 01:** [MKAFKA-01](sprint-01-fundamentals/MKAFKA-01-почему-kafka-и-playground.md) · [MKAFKA-02](sprint-01-fundamentals/MKAFKA-02-каркас-mcloud-orders.md) · [атлас](sprint-01-fundamentals/MKAFKA-02-атлас-файлов.md)

**Спринт 02:** [MKAFKA-03](sprint-02-producer-consumer/MKAFKA-03-producer-rest-kafka.md) · [атлас](sprint-02-producer-consumer/MKAFKA-03-атлас-файлов.md)

## Зачем этот курс

Мы учимся не «Kafka ради Kafka», а **событийной архитектуре на реальном домене** — сервис заказов **mcloud-orders** в экосистеме **m-CRM**.

Типичная боль без Kafka:

- сервис A создал заказ, сервис B узнал об этом через HTTP и упал — заказ «потерялся»;
- склад, биллинг и уведомления дергают один API заказов — он становится узким местом;
- при пиковой нагрузке синхронные вызовы каскадно валят всю систему.

Kafka решает это как **журнал событий**: заказ создали → записали событие → заинтересованные сервисы читают в своём темпе.

## Репозитории и роли

| Что | Где | Зачем |
|-----|-----|-------|
| **Теория, уроки, шаблоны** | Obsidian vault (`08-Kafka/kafka-mcloud-orders/`) | Читать, конспектировать, копировать шаблоны |
| **Практика (этот repo)** | `java-kafka` / `mcloud-orders` | Рабочий код, docker-compose, коммиты |

## Иерархия `docs/`

```
docs/
├── README.md
├── 00-план-курса-kafka.md
├── sprint-01-fundamentals/          ← MKAFKA-01, MKAFKA-02
├── sprint-02-producer-consumer/     ← MKAFKA-03 (+ атлас), MKAFKA-04 later
├── sprint-03-production/
├── api/
└── architecture/
```

## Структура курса

### Спринт 01 — Fundamentals → [`sprint-01-fundamentals/`](sprint-01-fundamentals/README.md)

| Урок | Тема | Статус |
|------|------|--------|
| **MKAFKA-01** | Playground Kafka | ✅ [урок](sprint-01-fundamentals/MKAFKA-01-почему-kafka-и-playground.md) |
| **MKAFKA-02** | Каркас Spring Boot | 📖 [урок](sprint-01-fundamentals/MKAFKA-02-каркас-mcloud-orders.md) · [атлас](sprint-01-fundamentals/MKAFKA-02-атлас-файлов.md) |

### Спринт 02 — Producer / Consumer → [`sprint-02-producer-consumer/`](sprint-02-producer-consumer/README.md)

| Урок | Тема | Статус |
|------|------|--------|
| **MKAFKA-03** | Producer REST → Kafka, топики по priority, метрики | 📖 [урок](sprint-02-producer-consumer/MKAFKA-03-producer-rest-kafka.md) · [атлас](sprint-02-producer-consumer/MKAFKA-03-атлас-файлов.md) |
| **MKAFKA-04** | Consumer + PostgreSQL | ⏳ скоро |

### Спринт 03 — Production → [`sprint-03-production/`](sprint-03-production/README.md)

Retry, DLQ, идемпотентность consumer, lag.

## Ключевые договорённости

- **Приложение:** `http://localhost:8081` (AKHQ на `8080`).
- **Брокер с хоста:** `localhost:9092`.
- **MKAFKA-03 топики:** `orders.priority.high` / `normal` / `low` (ключ = `region`).
- **MKAFKA-01/02** ранее: один топик `order-events` — для урока 3 используй priority‑топики из задания.

## Как двигаться дальше

1. Закрой MKAFKA‑02 (каркас собирается, local профиль, pgAdmin).
2. [MKAFKA-03](sprint-02-producer-consumer/MKAFKA-03-producer-rest-kafka.md) + [атлас](sprint-02-producer-consumer/MKAFKA-03-атлас-файлов.md).
3. MKAFKA‑04 — consumer.
