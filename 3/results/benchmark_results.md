# Результаты бенчмарка кеширования

| Стратегия | Профиль | Throughput (req/s) | Avg latency (ms) | P95 (ms) | DB ops | Hit rate % |
|-----------|---------|-------------------:|-----------------:|---------:|-------:|-----------:|
| cache_aside | read_heavy | 1191.4 | 6.71 | 19.32 | 13067 | 79.23 |
| cache_aside | balanced | 1161.87 | 6.88 | 18.76 | 26067 | 50.49 |
| cache_aside | write_heavy | 1170.77 | 6.83 | 17.21 | 33709 | 20.16 |
| write_through | read_heavy | 1163.73 | 6.87 | 19.54 | 7067 | 100.0 |
| write_through | balanced | 1118.03 | 7.15 | 18.89 | 16743 | 100.0 |
| write_through | write_heavy | 1082.9 | 7.38 | 18.32 | 25975 | 100.0 |
| write_back | read_heavy | 1169.27 | 6.84 | 20.45 | 1087 | 98.98 |
| write_back | balanced | 1109.8 | 7.2 | 21.03 | 850 | 100.0 |
| write_back | write_heavy | 1038.17 | 7.69 | 23.18 | 900 | 100.0 |
