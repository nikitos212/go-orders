# go-orders — локальная разработка и запуск

## 1. Требования

- Docker + docker-compose
- Go 1.20+ (или версия, с которой вы работаете)
- PostgreSQL (опционально; можно выполнять миграции внутри контейнера)
- (опционально) tmux/screen или любая возможность держать параллельные терминалы

---

## 2. Быстрый старт

### 2.1 Запустить инфраструктуру (Kafka + Zookeeper + Postgres)

В корне репозитория должен быть файл `docker-compose.yml`. Поднимите сервисы:

```bash
docker-compose up -d
docker ps
```

Ожидаемые контейнеры: `zookeeper`, `kafka`, `postgres`.

Если Kafka контейнер не запустился (статус `exited`) — посмотрите логи:

```bash
docker logs <kafka-container> --tail 200
# или
docker logs go-orders_kafka_1 --tail 200
```

Если в логах видите ошибки типа `NodeExists` и Kafka падает — попробуйте полностью перезапустить с очисткой volume:

> **Внимание:** `-v` удалит персистентные данные контейнеров.

```bash
docker-compose down -v
docker-compose up -d
```

---

### 2.2 Применить миграции (создать таблицы)

В репозитории есть файл миграции `migrations/001_init.sql`. Применить можно двумя способами.

**A. На хосте (если установлен `psql`)**

```bash
psql "postgresql://orders_user:secret@localhost:5432/orders_db" -f migrations/001_init.sql
```

**B. В контейнере Postgres**

```bash
docker ps
docker cp migrations/001_init.sql <postgres-container>:/tmp/001_init.sql
docker exec -it <postgres-container> psql -U orders_user -d orders_db -f /tmp/001_init.sql
```

Проверить таблицы и записи в `psql`:

```sql
\dt
SELECT count(*) FROM orders;
```

---

### 2.3 Создать Kafka-топики

Подставьте имя своего kafka-контейнера (`docker ps`) вместо `<kafka-container>`.

```bash
# основной топик (если ещё не создан)
docker exec -it <kafka-container> /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# топик для lookup (API -> consumer)
docker exec -it <kafka-container> /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --topic order-lookup --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# топик для ответов (consumer -> API)
docker exec -it <kafka-container> /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --topic order-found --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# проверить список топиков
docker exec -it <kafka-container> /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## 3. Подготовка и запуск Go-приложений

### 3.1 Инициализация модуля и зависимости (один раз)

В корне проекта:

```bash
# подставьте свой модуль (например github.com/youruser/go-orders)
go mod init github.com/youruser/go-orders

# зависимости
go get github.com/segmentio/kafka-go
go get github.com/jackc/pgx/v5/pgxpool
go get github.com/jackc/pgx/v5
go mod tidy
```

---

### 3.2 Структура запуска

- **Терминал A:** consumer

```bash
# если у вас есть cmd/consumer/main.go
go run ./cmd/consumer
# или, если consumer.go в корне
go run consumer.go
```

- **Терминал B:** api

```bash
go run ./cmd/api
# или
go run api.go
```

---

### 4.1 Отправка нового заказа (frontend → API → Kafka orders → consumer → Postgres + cache)

```bash
curl -v -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d @migrations/test_order.json
```

**Ожидаемый ответ:** `202 Accepted` и JSON вида:

```json
{"status":"accepted","order_uid":"..."}
```

В логах consumer должно появиться сообщение о сохранении, например:

```
processed order <order_uid> (offset ...)
```

Проверить в БД:

```bash
psql "postgresql://orders_user:secret@localhost:5432/orders_db"
SELECT * FROM orders WHERE order_uid='b563feb7b2b84b6test';
```

---

### 4.2 Получение заказа через API (cache hit)

Если заказ есть в кешe (после POST он кладётся в кеш API), сделайте запрос:

```bash
curl http://localhost:8080/api/order/b563feb7b2b84b6test
```

Ожидаемый результат: JSON заказа (из кеша).

---

### 4.3 Lookup flow (cache miss → Kafka lookup → DB → order-found → cache updated)

Если вы очистили кеш (или перезапустили API) и сделали `GET` для существующего в БД `order_uid`, API отправит сообщение в `order-lookup` топик, consumer прочитает, выполнит `GetOrderFromDB`, и опубликует `order-found` с полным JSON. API, получив `order-found`, положит данные в кеш.

Пример сценария:

1. Очистите кеш (или перезапустите API).
2. Выполните запрос:

```bash
curl -v http://localhost:8080/api/order/b563feb7b2b84b6test
```

При промахе API вернёт `202 lookup_queued` (или, при быстром ответе от consumer, может вернуть полный ответ).

**Примеры логов:**

- В логах consumer:

```
LOOKUP RECEIVED order_uid=b563... action=query-db
DB QUERY order_uid=b563...
DB FOUND order_uid=b563...
published order-found for b563...
```

- В логах API:

```
CACHE UPDATED order_uid=b563... source=order-found
```

Повторите `GET` — теперь ответ вернёт заказ из кеша.

---

## Полезные заметки

- Если в процессе поднятия Kafka вы получаете ошибки `NodeExists` или похожие — чаще всего помогает `docker-compose down -v` и повторный `up`.
- Проверьте конфигурации подключений (адреса, порты, учётные данные) в `config`/env-файлах проекта.
- Логируйте offset'ы и ошибки в consumer для упрощения отладки.

---

