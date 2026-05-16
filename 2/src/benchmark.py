import argparse
import asyncio
import json
import math
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

import aio_pika
import redis.asyncio as redis


@dataclass
class TestConfiguration:
    broker_type: str
    msg_size_bytes: int
    messages_per_second: int
    test_duration_sec: int
    num_producers: int
    num_consumers: int
    queue_identifier: str
    rabbitmq_connection_url: str
    redis_connection_url: str
    drain_period_sec: int


@dataclass
class TestMetrics:
    broker_type: str
    msg_size_bytes: int
    messages_per_second: int
    test_duration_sec: int
    num_producers: int
    num_consumers: int
    total_sent: int
    failed_sends: int
    total_received: int
    messages_lost: int
    send_throughput: float
    receive_throughput: float
    latency_avg_ms: float
    latency_p95_ms: float
    latency_max_ms: float
    queue_backlog: int
    is_degraded: bool


def split_csv_values(input_str: str) -> List[int]:
    return [int(val.strip()) for val in input_str.split(",") if val.strip()]


def create_test_message(size: int, sequence: int) -> bytes:
    payload_data = {"seq": sequence, "sent_ts": time.time_ns(), "payload": ""}
    encoded = json.dumps(payload_data, separators=(",", ":")).encode("utf-8")
    header_size = len(encoded) + 2
    fill_size = max(0, size - header_size)
    payload_data["payload"] = "x" * fill_size
    message = json.dumps(payload_data, separators=(",", ":")).encode("utf-8")
    return message[:size]


def calculate_percentile(data: List[float], pct: float) -> float:
    if not data:
        return 0.0
    if len(data) == 1:
        return data[0]
    sorted_data = sorted(data)
    position = min(len(sorted_data) - 1, max(0, math.ceil((pct / 100.0) * len(sorted_data)) - 1))
    return sorted_data[position]


class RabbitMQAdapter:
    def __init__(self, connection_url: str, queue_id: str):
        self.connection_url = connection_url
        self.queue_id = queue_id
        self.conn = None
        self.ch = None
        self.q = None

    async def initialize(self):
        retry_count = 10
        last_exception = None
        for retry in range(retry_count):
            try:
                self.conn = await aio_pika.connect_robust(self.connection_url, timeout=5)
                self.ch = await self.conn.channel()
                await self.ch.set_qos(prefetch_count=1000)
                self.q = await self.ch.declare_queue(self.queue_id, durable=False, auto_delete=True)
                await self.q.purge()
                return
            except Exception as ex:
                last_exception = ex
                await asyncio.sleep(1 + retry * 0.5)
        raise RuntimeError(f"Failed to connect to RabbitMQ: {last_exception}")

    async def publish_message(self, data: bytes):
        message = aio_pika.Message(body=data, delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT)
        await self.ch.default_exchange.publish(message, routing_key=self.queue_id)

    async def start_consuming(self, stop_signal: asyncio.Event, latency_list: List[float], stats: dict):
        async with self.q.iterator() as msg_iterator:
            async for msg in msg_iterator:
                async with msg.process(ignore_processed=True):
                    recv_time = time.time_ns()
                    try:
                        parsed = json.loads(msg.body)
                        send_timestamp = int(parsed["sent_ts"])
                        latency_list.append((recv_time - send_timestamp) / 1_000_000)
                        stats["received"] += 1
                    except Exception:
                        stats["decode_errors"] += 1
                if stop_signal.is_set():
                    break

    async def get_queue_size(self) -> int:
        queue_info = await self.ch.declare_queue(self.queue_id, durable=False, passive=True)
        return queue_info.declaration_result.message_count

    async def cleanup(self):
        if self.conn:
            await self.conn.close()


class RedisAdapter:
    def __init__(self, connection_url: str, queue_id: str):
        self.connection_url = connection_url
        self.queue_id = queue_id
        self.redis_client = None

    async def initialize(self):
        self.redis_client = redis.from_url(self.connection_url, decode_responses=False)
        retry_count = 10
        last_exception = None
        for retry in range(retry_count):
            try:
                await self.redis_client.ping()
                await self.redis_client.delete(self.queue_id)
                return
            except Exception as ex:
                last_exception = ex
                await asyncio.sleep(0.5 + retry * 0.3)
        raise RuntimeError(f"Failed to connect to Redis: {last_exception}")

    async def publish_message(self, data: bytes):
        await self.redis_client.rpush(self.queue_id, data)

    async def start_consuming(self, stop_signal: asyncio.Event, latency_list: List[float], stats: dict):
        while True:
            result = await self.redis_client.blpop(self.queue_id, timeout=1)
            if result is not None:
                _, message_data = result
                recv_time = time.time_ns()
                try:
                    parsed = json.loads(message_data)
                    send_timestamp = int(parsed["sent_ts"])
                    latency_list.append((recv_time - send_timestamp) / 1_000_000)
                    stats["received"] += 1
                except Exception:
                    stats["decode_errors"] += 1
            if stop_signal.is_set() and result is None:
                break

    async def get_queue_size(self) -> int:
        return await self.redis_client.llen(self.queue_id)

    async def cleanup(self):
        if self.redis_client:
            await self.redis_client.aclose()


async def run_producer_task(adapter, config: TestConfiguration, producer_index: int, stats: dict):
    target_rate = config.messages_per_second / config.num_producers
    send_interval = 1.0 / max(target_rate, 1)
    end_time = time.perf_counter() + config.test_duration_sec
    seq_number = producer_index * 10_000_000

    while time.perf_counter() < end_time:
        start_ts = time.perf_counter()
        message = create_test_message(config.msg_size_bytes, seq_number)
        try:
            await adapter.publish_message(message)
            stats["sent"] += 1
        except Exception:
            stats["send_errors"] += 1
        seq_number += 1
        time_taken = time.perf_counter() - start_ts
        wait_time = send_interval - time_taken
        if wait_time > 0:
            await asyncio.sleep(wait_time)


async def execute_benchmark(config: TestConfiguration) -> TestMetrics:
    if config.broker_type == "rabbitmq":
        adapter = RabbitMQAdapter(config.rabbitmq_connection_url, config.queue_identifier)
    elif config.broker_type == "redis":
        adapter = RedisAdapter(config.redis_connection_url, config.queue_identifier)
    else:
        raise ValueError(f"Unknown broker type: {config.broker_type}")

    await adapter.initialize()
    latency_measurements: List[float] = []
    statistics_counters = {"sent": 0, "send_errors": 0, "received": 0, "decode_errors": 0}
    stop_signal = asyncio.Event()

    consumer_tasks = [
        asyncio.create_task(adapter.start_consuming(stop_signal, latency_measurements, statistics_counters))
        for _ in range(config.num_consumers)
    ]

    test_start = time.perf_counter()
    producer_tasks = [
        asyncio.create_task(run_producer_task(adapter, config, idx, statistics_counters))
        for idx in range(config.num_producers)
    ]
    await asyncio.gather(*producer_tasks)

    await asyncio.sleep(config.drain_period_sec)
    stop_signal.set()
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    test_elapsed = time.perf_counter() - test_start

    remaining_messages = await adapter.get_queue_size()
    await adapter.cleanup()

    sent_count = statistics_counters["sent"]
    received_count = statistics_counters["received"]
    lost_count = max(0, sent_count - received_count)
    mean_latency = statistics.mean(latency_measurements) if latency_measurements else 0.0
    p95_latency = calculate_percentile(latency_measurements, 95)
    maximum_latency = max(latency_measurements) if latency_measurements else 0.0
    send_rate = sent_count / test_elapsed if test_elapsed else 0.0
    receive_rate = received_count / test_elapsed if test_elapsed else 0.0

    degradation_detected = (
            remaining_messages > config.messages_per_second * 2
            or p95_latency > 200
            or statistics_counters["send_errors"] > 0
            or received_count < sent_count * 0.98
    )

    return TestMetrics(
        broker_type=config.broker_type,
        msg_size_bytes=config.msg_size_bytes,
        messages_per_second=config.messages_per_second,
        test_duration_sec=config.test_duration_sec,
        num_producers=config.num_producers,
        num_consumers=config.num_consumers,
        total_sent=sent_count,
        failed_sends=statistics_counters["send_errors"],
        total_received=received_count,
        messages_lost=lost_count,
        send_throughput=send_rate,
        receive_throughput=receive_rate,
        latency_avg_ms=mean_latency,
        latency_p95_ms=p95_latency,
        latency_max_ms=maximum_latency,
        queue_backlog=remaining_messages,
        is_degraded=degradation_detected,
    )


def format_csv_line(metrics: TestMetrics) -> str:
    values = [
        metrics.broker_type,
        str(metrics.msg_size_bytes),
        str(metrics.messages_per_second),
        str(metrics.test_duration_sec),
        str(metrics.num_producers),
        str(metrics.num_consumers),
        str(metrics.total_sent),
        str(metrics.failed_sends),
        str(metrics.total_received),
        str(metrics.messages_lost),
        f"{metrics.send_throughput:.2f}",
        f"{metrics.receive_throughput:.2f}",
        f"{metrics.latency_avg_ms:.2f}",
        f"{metrics.latency_p95_ms:.2f}",
        f"{metrics.latency_max_ms:.2f}",
        str(metrics.queue_backlog),
        str(metrics.is_degraded),
    ]
    return ",".join(values)


def generate_markdown_report(all_metrics: List[TestMetrics]) -> str:
    report_lines = [
        "# Сравнительный анализ производительности брокеров сообщений",
        "",
        "| Брокер | Размер (байт) | Поток (msg/s) | Отправлено | Получено | Потеряно | Прием (msg/s) | Средн. (ms) | P95 (ms) | Макс. (ms) | Очередь | Деградация |",
        "|--------|---------------|---------------|------------|----------|----------|---------------|-------------|----------|------------|---------|------------|",
    ]
    for m in all_metrics:
        report_lines.append(
            f"| {m.broker_type} | {m.msg_size_bytes} | {m.messages_per_second} | {m.total_sent} | {m.total_received} | {m.messages_lost} | "
            f"{m.receive_throughput:.2f} | {m.latency_avg_ms:.2f} | {m.latency_p95_ms:.2f} | {m.latency_max_ms:.2f} | "
            f"{m.queue_backlog} | {m.is_degraded} |"
        )
    return "\n".join(report_lines) + "\n"


async def main():
    arg_parser = argparse.ArgumentParser(description="Message broker performance comparison tool")
    arg_parser.add_argument("--brokers", default="rabbitmq,redis")
    arg_parser.add_argument("--sizes", default="128,1024,10240,102400")
    arg_parser.add_argument("--rates", default="1000,5000,10000")
    arg_parser.add_argument("--duration", type=int, default=30)
    arg_parser.add_argument("--producers", type=int, default=2)
    arg_parser.add_argument("--consumers", type=int, default=2)
    arg_parser.add_argument("--drain-timeout", type=int, default=5)
    arg_parser.add_argument("--rabbit-url", default="amqp://guest:guest@localhost/")
    arg_parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    arg_parser.add_argument("--queue-prefix", default="bench.queue")
    arg_parser.add_argument("--out-dir", default="results")
    arg_parser.add_argument(
        "--quick",
        action="store_true",
        help="Запустить быстрый тест для проверки работоспособности (2 размера x 2 потока x 10сек)",
    )
    args = arg_parser.parse_args()

    broker_list = [b.strip() for b in args.brokers.split(",") if b.strip()]
    size_list = split_csv_values(args.sizes)
    rate_list = split_csv_values(args.rates)
    test_duration = args.duration

    if args.quick:
        size_list = [128, 1024]
        rate_list = [1000, 5000]
        test_duration = 10
    output_directory = Path(args.out_dir)
    output_directory.mkdir(parents=True, exist_ok=True)

    csv_header = (
        "broker,message_size,rate,duration_s,producers,consumers,sent,send_errors,received,lost,"
        "throughput_sent,throughput_received,avg_latency_ms,p95_latency_ms,max_latency_ms,backlog_end,degraded"
    )

    all_test_results: List[TestMetrics] = []
    csv_content = [csv_header]

    total_tests = len(broker_list) * len(size_list) * len(rate_list)
    current_test = 0

    for broker_name in broker_list:
        for msg_size in size_list:
            for msg_rate in rate_list:
                current_test += 1
                queue_id = f"{args.queue_prefix}.{broker_name}.{msg_size}.{msg_rate}"
                test_config = TestConfiguration(
                    broker_type=broker_name,
                    msg_size_bytes=msg_size,
                    messages_per_second=msg_rate,
                    test_duration_sec=test_duration,
                    num_producers=args.producers,
                    num_consumers=args.consumers,
                    queue_identifier=queue_id,
                    rabbitmq_connection_url=args.rabbit_url,
                    redis_connection_url=args.redis_url,
                    drain_period_sec=args.drain_timeout,
                )
                print(
                    f"[Тест {current_test}/{total_tests}] Брокер={broker_name} "
                    f"Размер={msg_size}Б Поток={msg_rate}msg/s Длительность={test_duration}с"
                )
                test_result = await execute_benchmark(test_config)
                all_test_results.append(test_result)
                csv_content.append(format_csv_line(test_result))

    csv_output_path = output_directory / "benchmark_results.csv"
    markdown_output_path = output_directory / "benchmark_results.md"
    csv_output_path.write_text("\n".join(csv_content) + "\n", encoding="utf-8")
    markdown_output_path.write_text(generate_markdown_report(all_test_results), encoding="utf-8")
    print(f"Результаты сохранены: {csv_output_path}")
    print(f"Результаты сохранены: {markdown_output_path}")


if __name__ == "__main__":
    asyncio.run(main())
