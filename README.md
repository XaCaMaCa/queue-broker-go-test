# In-memory HTTP queue broker (Go)

[![CI](https://github.com/XaCaMaCa/queue-broker-go-test/actions/workflows/ci.yml/badge.svg)](https://github.com/XaCaMaCa/queue-broker-go-test/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.22-blue.svg)](https://go.dev/)

Минимальный HTTP-брокер очередей в памяти: **FIFO**, **long-polling** с таймаутом, **per-queue** воркер (одна горутина на очередь), **graceful shutdown** (SIGINT/SIGTERM), структурные логи через `slog`.

Изначально тестовое задание; дальше доработано под продакшен-практики: тесты (unit, HTTP, конкурентность), корректное завершение и ответ **503** при остановке.

## API

Базовый путь: `/{queueName}` — имя очереди одним сегментом (без `/` внутри).

| Метод | URL | Описание |
|--------|-----|----------|
| `PUT` | `/{queue}?v=<payload>` | Положить сообщение в очередь. Параметр `v` обязателен (значение может быть пустым: `?v=`). |
| `GET` | `/{queue}` | Взять сообщение без ожидания. Пустая очередь → **404**. |
| `GET` | `/{queue}?timeout=<секунды>` | Long-poll: ждать до `timeout` секунд. Сообщение пришло → **200** и тело. Таймаут → **404**. Некорректный `timeout` → **400**. |
| другие методы | `/{queue}` | **405** |

При остановке сервера после сигнала: новые `PUT`/`GET` с ожиданием → **503 Service Unavailable**.

## Запуск

```bash
go run . 8080
```

Пример:

```bash
curl -X PUT "http://localhost:8080/orders?v=hello"
curl "http://localhost:8080/orders"
```

Long-poll:

```bash
curl "http://localhost:8080/orders?timeout=30"
```

Остановка: **Ctrl+C** (или SIGTERM). Сервер завершит `http.Server.Shutdown`, закроет брокер и разбудит ожидающих потребителей.

## Тесты

```bash
go test -v ./...
```

С race detector на Linux/macOS (нужен `gcc` / cgo):

```bash
go test -race -v ./...
```

На Windows без MinGW/`gcc` локально удобно гонять без `-race`; в CI прогон с `-race` включён.

Если в родительской папке есть `go.work` и модуль в него не добавлен:

```powershell
$env:GOWORK = "off"
go test -v ./...
```

## Архитектура

Очередь — **actor**: все изменения состояния проходят через канал `chan func(*qSt)` и одну горутину-воркер на очередь. Снаружи — `sync.Mutex` только на карте очередей в `broker`.

```mermaid
flowchart TB
  HTTP[net/http] --> H[makeQueueHandler]
  H --> B[broker.queue name]
  B --> Q[queue per name]
  Q --> W[worker goroutine]
  W --> S[qSt: msgs + waiters]
```

```mermaid
sequenceDiagram
  participant C as Client
  participant H as HTTP handler
  participant Q as queue worker
  participant S as qSt

  C->>H: PUT /q?v=msg
  H->>Q: enqueue via execSync
  Q->>S: append msg or wake waiter
  H-->>C: 200

  C->>H: GET /q?timeout=N
  H->>Q: dequeueWait
  Q->>S: register waiter OR take msg
  alt message available
    Q-->>H: msg
    H-->>C: 200 + body
  else timeout
    Q-->>H: errTimeout
    H-->>C: 404
  end
```

## Структура репозитория

| Файл | Назначение |
|------|------------|
| `main.go` | Брокер, очередь, HTTP, graceful shutdown, `slog` |
| `broker_test.go` | Unit-тесты FIFO, таймаут, waiters, shutdown |
| `http_test.go` | Интеграционные тесты через `httptest` |
| `concurrent_test.go` | Конкурентные сценарии без потери сообщений |

## Модуль

```text
module queuebroker
go 1.22
```

## Лицензия

MIT (при необходимости добавьте файл `LICENSE`).
