#!/usr/bin/env python3

import argparse
import asyncio
import csv
import json
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List
import httpx

SERVICE_ENDPOINTS = {
    "cache_aside": "http://localhost:8001",
    "write_through": "http://localhost:8002",
    "write_back": "http://localhost:8003",
}

LOAD_PROFILES = {
    "read_heavy": 0.80,
    "balanced": 0.50,
    "write_heavy": 0.20,
}


@dataclass
class BenchmarkResult:
    strategy: str
    profile: str
    duration_s: int
    target_rps: int
    total_requests: int
    read_requests: int
    write_requests: int
    errors: int
    throughput_rps: float
    avg_latency_ms: float
    p95_latency_ms: float
    db_reads: int
    db_writes: int
    db_total: int
    cache_hit_rate_pct: float
    write_back_flushes: int
    write_back_flushed_items: int


def calculate_percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    index = min(len(sorted_data) - 1, max(0, int(len(sorted_data) * p / 100) - 1))
    return sorted_data[index]


async def wait_for_service(client: httpx.AsyncClient, url: str, timeout: float = 60) -> None:
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            response = await client.get(f"{url}/health", timeout=2.0)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.5)
    raise RuntimeError(f"Service unavailable: {url}")


async def execute_benchmark(
        strategy_name: str,
        endpoint_url: str,
        profile_name: str,
        read_probability: float,
        test_duration: int,
        requests_per_sec: int,
        key_pool_size: int,
        worker_count: int,
) -> BenchmarkResult:
    latency_samples: List[float] = []
    error_count = 0
    read_count = 0
    write_count = 0
    end_timestamp = time.time() + test_duration
    request_interval = 1.0 / requests_per_sec if requests_per_sec > 0 else 0

    async with httpx.AsyncClient(timeout=10.0) as client:
        await wait_for_service(client, endpoint_url)
        await client.post(f"{endpoint_url}/admin/reset-metrics")
        if strategy_name == "write_back":
            await client.post(f"{endpoint_url}/admin/flush")

        async def worker_task(worker_id: int) -> None:
            nonlocal error_count, read_count, write_count
            rand_gen = random.Random(worker_id + int(time.time()))
            next_request_time = time.time()
            while time.time() < end_timestamp:
                current_time = time.time()
                if current_time < next_request_time:
                    await asyncio.sleep(min(0.001, next_request_time - current_time))
                    continue
                next_request_time += request_interval

                key_id = rand_gen.randint(1, key_pool_size)
                is_read_op = rand_gen.random() < read_probability
                start_time = time.perf_counter()
                try:
                    if is_read_op:
                        resp = await client.get(f"{endpoint_url}/items/{key_id}")
                        read_count += 1
                    else:
                        data_value = f"w-{strategy_name}-{int(time.time() * 1000) % 100000}"
                        resp = await client.put(f"{endpoint_url}/items/{key_id}", json={"value": data_value})
                        write_count += 1
                    if resp.status_code >= 400:
                        error_count += 1
                    else:
                        latency_samples.append((time.perf_counter() - start_time) * 1000)
                except httpx.HTTPError:
                    error_count += 1

        workers = [asyncio.create_task(worker_task(i)) for i in range(worker_count)]
        await asyncio.gather(*workers)

        if strategy_name == "write_back":
            await asyncio.sleep(3)
            await client.post(f"{endpoint_url}/admin/flush")

        metrics_response = await client.get(f"{endpoint_url}/metrics")
        metrics_response.raise_for_status()
        metrics_data = metrics_response.json()

    total_ops = read_count + write_count
    elapsed_time = test_duration
    return BenchmarkResult(
        strategy=strategy_name,
        profile=profile_name,
        duration_s=test_duration,
        target_rps=requests_per_sec,
        total_requests=total_ops,
        read_requests=read_count,
        write_requests=write_count,
        errors=error_count,
        throughput_rps=round(total_ops / elapsed_time, 2),
        avg_latency_ms=round(statistics.mean(latency_samples), 2) if latency_samples else 0.0,
        p95_latency_ms=round(calculate_percentile(latency_samples, 95), 2),
        db_reads=metrics_data.get("db_reads", 0),
        db_writes=metrics_data.get("db_writes", 0),
        db_total=metrics_data.get("db_total", 0),
        cache_hit_rate_pct=metrics_data.get("cache_hit_rate_pct", 0.0),
        write_back_flushes=metrics_data.get("write_back_flushes", 0),
        write_back_flushed_items=metrics_data.get("write_back_flushed_items", 0),
    )


def display_result(result: BenchmarkResult) -> None:
    print(
        f"[{result.strategy:14}] profile={result.profile:12} "
        f"req={result.total_requests:5} thr={result.throughput_rps:7.1f} rps "
        f"avg={result.avg_latency_ms:6.2f}ms p95={result.p95_latency_ms:6.2f}ms "
        f"db={result.db_total:5} hit={result.cache_hit_rate_pct:5.1f}% "
        f"errors={result.errors}"
    )
    if result.strategy == "write_back":
        print(
            f"    write-back: flushes={result.write_back_flushes} "
            f"flushed_items={result.write_back_flushed_items}"
        )


def export_results(results: List[BenchmarkResult], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_file = output_dir / "benchmark_results.csv"
    markdown_file = output_dir / "benchmark_results.md"

    columns = [
        "strategy",
        "profile",
        "duration_s",
        "target_rps",
        "total_requests",
        "read_requests",
        "write_requests",
        "throughput_rps",
        "avg_latency_ms",
        "p95_latency_ms",
        "db_reads",
        "db_writes",
        "db_total",
        "cache_hit_rate_pct",
        "write_back_flushes",
        "write_back_flushed_items",
        "errors",
    ]

    with csv_file.open("w", newline="", encoding="utf-8") as f:
        csv_writer = csv.DictWriter(f, fieldnames=columns)
        csv_writer.writeheader()
        for result in results:
            csv_writer.writerow({col: getattr(result, col) for col in columns})

    markdown_lines = [
        "# Результаты бенчмарка кеширования",
        "",
        "| Стратегия | Профиль | Throughput (req/s) | Avg latency (ms) | P95 (ms) | DB ops | Hit rate % |",
        "|-----------|---------|-------------------:|-----------------:|---------:|-------:|-----------:|",
    ]
    for result in results:
        markdown_lines.append(
            f"| {result.strategy} | {result.profile} | {result.throughput_rps} | "
            f"{result.avg_latency_ms} | {result.p95_latency_ms} | {result.db_total} | {result.cache_hit_rate_pct} |"
        )

    markdown_file.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    json_file = output_dir / "benchmark_results.json"
    json_file.write_text(
        json.dumps([{col: getattr(r, col) for col in columns} for r in results], indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved: {csv_file}, {markdown_file}, {json_file}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Cache strategy benchmark")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--rps", type=int, default=200)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--item-pool", type=int, default=500)
    parser.add_argument("--quick", action="store_true", help="Short run: 10s, 100 rps")
    parser.add_argument("--out-dir", type=str, default="results")
    parser.add_argument(
        "--strategies",
        type=str,
        default="cache_aside,write_through,write_back",
    )
    args = parser.parse_args()

    if args.quick:
        args.duration = 10
        args.rps = 100

    selected_strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]
    all_results: List[BenchmarkResult] = []

    print(
        f"Benchmark: duration={args.duration}s rps={args.rps} "
        f"workers={args.workers} pool={args.item_pool}"
    )
    print("=" * 90)

    for strategy in selected_strategies:
        service_url = SERVICE_ENDPOINTS[strategy]
        for profile, read_ratio in LOAD_PROFILES.items():
            print(f"\n>>> {strategy} / {profile} (read={read_ratio:.0%})")
            result = await execute_benchmark(
                strategy_name=strategy,
                endpoint_url=service_url,
                profile_name=profile,
                read_probability=read_ratio,
                test_duration=args.duration,
                requests_per_sec=args.rps,
                key_pool_size=args.item_pool,
                worker_count=args.workers,
            )
            display_result(result)
            all_results.append(result)

    export_results(all_results, Path(args.out_dir))


if __name__ == "__main__":
    asyncio.run(main())
