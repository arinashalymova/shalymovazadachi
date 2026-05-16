#!/usr/bin/env bash
set -euo pipefail

echo "Создание виртуального окружения..."
python3 -m venv .venv
source .venv/bin/activate

echo "Установка зависимостей..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Запуск контейнеров брокеров..."
docker compose up -d rabbitmq redis

echo "Ожидание готовности RabbitMQ..."
for attempt in {1..30}; do
  if docker exec mq-rabbitmq rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
    echo "RabbitMQ готов!"
    break
  fi
  sleep 2
done

echo "Ожидание готовности Redis..."
for attempt in {1..30}; do
  if docker exec mq-redis redis-cli ping >/dev/null 2>&1; then
    echo "Redis готов!"
    break
  fi
  sleep 1
done

echo "Запуск тестирования..."
python src/benchmark.py \
  --brokers rabbitmq,redis \
  --sizes 128,1024,10240,102400 \
  --rates 1000,5000,10000 \
  --duration 30 \
  --producers 2 \
  --consumers 2 \
  --drain-timeout 5 \
  --out-dir results

echo "Тестирование завершено!"
