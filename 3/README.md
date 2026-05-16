# Сравнение стратегий кеширования

Проект для сравнения трёх подходов к кешированию на базе одной системы (ключ-значение):

| Сервис | Порт | Стратегия |
|--------|------|-----------|
| `app-cache-aside` | 8001 | Cache-Aside (Lazy Loading) |
| `app-write-through` | 8002 | Write-Through |
| `app-write-back` | 8003 | Write-Back |

**Стек:** PostgreSQL, Redis, FastAPI, Python

## Запуск

```bash
chmod +x run.sh
./run.sh          # полный тест: 30 секунд, 200 RPS
./run.sh --quick  # быстрый тест: 10 секунд, 100 RPS
```

Результаты сохраняются в `results/`.

## API

- `GET /items/{id}` — получить элемент
- `PUT /items/{id}` — обновить элемент
- `GET /metrics` — метрики (hit rate, обращения к БД)
- `POST /admin/reset-metrics` — сброс метрик
- `POST /admin/flush` — принудительный сброс (Write-Back)

## Тестовые профили

Для каждой стратегии запускается 3 профиля нагрузки:

- `read_heavy` — 80% чтение, 20% запись
- `balanced` — 50% / 50%
- `write_heavy` — 20% чтение, 80% запись

## Структура

```
app/                  # FastAPI приложение
load_generator/       # генератор нагрузки
docker/               # SQL инициализация
results/              # результаты тестов
REPORT.md             # итоговый отчёт
```

## Git

```bash
git init
git add .
git commit -m "Сравнение Cache-Aside, Write-Through, Write-Back"
git remote add origin <URL>
git push -u origin main
```
