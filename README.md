# mcloud-orders — практический репозиторий курса Kafka

Локальный playground для курса **Kafka для Java-разработчика** (домен m-CRM).

## Быстрый старт инфраструктуры

```bash
docker run --rm confluentinc/cp-kafka:7.7.1 kafka-storage random-uuid
cp .env.example .env
docker compose up -d
```

| Сервис | Адрес |
|--------|--------|
| Kafka | `localhost:9092` |
| PostgreSQL | `localhost:5432` (orders/orders/orders) |
| AKHQ | http://localhost:8080 |
| Приложение | http://localhost:8081 |
| pgAdmin | http://localhost:5050 |

## Запуск приложения

```bash
./gradlew.bat bootRun --args="--spring.profiles.active=local"
```

## Проверка отправки (MKAFKA-03)

```bash
curl -X POST http://localhost:8081/api/v1/orders ^
  -H "Content-Type: application/json" ^
  -d "{\"customerId\":\"550e8400-e29b-41d4-a716-446655440000\",\"priority\":\"HIGH\",\"region\":\"EU\",\"amount\":1200.50,\"lines\":[{\"productId\":\"6ba7b814-9dad-11d1-80b4-00c04fd430c8\",\"quantity\":2,\"price\":600.25}]}"

curl http://localhost:8081/api/v1/orders/metrics
```

В AKHQ: Topics → `orders.priority.high` → сообщение с key=`EU`.

```bash
./gradlew.bat test --tests "ru.mentee.power.orders.domain.usecase.PlaceOrderUseCaseTest"
```

## Документация

| Файл | Описание |
|------|----------|
| [docs/README.md](docs/README.md) | Оглавление |
| [MKAFKA-03 урок](docs/sprint-02-producer-consumer/MKAFKA-03-producer-rest-kafka.md) | Теория + азбука портов/адаптеров/record |
| [MKAFKA-03 атлас](docs/sprint-02-producer-consumer/MKAFKA-03-атлас-файлов.md) | Разбор файлов |
| [OpenAPI 0.2.0](docs/api/order-api.yaml) | Контракт API |
| [Пример metrics](docs/examples/metrics-response.json) | Пример ответа |

## Статус

- [x] MKAFKA-01 — инфраструктура
- [x] MKAFKA-02 — каркас
- [x] MKAFKA-03 — producer (код + уроки)
- [ ] MKAFKA-04 — consumer
