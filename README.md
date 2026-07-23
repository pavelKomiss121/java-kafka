# mcloud-orders — практический репозиторий курса Kafka

Локальный playground для курса **Kafka для Java-разработчика** (домен m-CRM, сервис заказов).

## Быстрый старт

```bash
# 1. UUID для KRaft-кластера
docker run --rm confluentinc/cp-kafka:7.7.1 kafka-storage random-uuid

# 2. Создать .env из шаблона и вставить UUID
cp .env.example .env

# 3. Поднять стенд
docker compose up -d
```

- **Kafka** (с хоста): `localhost:9092`
- **PostgreSQL**: `localhost:5432` (db/user/pass: `orders`)
- **AKHQ** (UI): http://localhost:8080

## Документация

Начни с оглавления: **[docs/README.md](docs/README.md)**

| Файл | Описание |
|------|----------|
| [docs/00-план-курса-kafka.md](docs/00-план-курса-kafka.md) | Карта курса, спринты |
| [docs/sprint-01-fundamentals/](docs/sprint-01-fundamentals/README.md) | **Спринт 01** — MKAFKA-01, MKAFKA-02 |
| [MKAFKA-01](docs/sprint-01-fundamentals/MKAFKA-01-почему-kafka-и-playground.md) | Урок 01 — playground |
| [MKAFKA-02](docs/sprint-01-fundamentals/MKAFKA-02-каркас-mcloud-orders.md) | Урок 02 — каркас Spring Boot |
| [Атлас файлов](docs/sprint-01-fundamentals/MKAFKA-02-атлас-файлов.md) | Как создавать каждый файл |
| [docs/sprint-02-producer-consumer/](docs/sprint-02-producer-consumer/README.md) | Спринт 02 (пока пусто) |
| [docs/api/](docs/api/README.md) | OpenAPI |
| [docs/architecture/](docs/architecture/README.md) | Диаграммы |

## Текущий статус

- [x] MKAFKA-01 — инфраструктура (Kafka KRaft + Postgres + AKHQ)
- [ ] MKAFKA-02 — Spring Boot-каркас (урок в `docs/sprint-01-fundamentals/`, код ещё не собран)
- [ ] MKAFKA-03+ — producer/consumer → `docs/sprint-02-producer-consumer/`
