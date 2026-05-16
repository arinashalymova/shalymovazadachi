from dataclasses import dataclass, field
from threading import Lock


@dataclass
class MetricsCollector:
    hits: int = 0
    misses: int = 0
    reads: int = 0
    writes: int = 0
    dirty_count: int = 0
    flush_count: int = 0
    flushed_total: int = 0
    _mutex: Lock = field(default_factory=Lock, repr=False)

    def increment(self, metric_name: str, amount: int = 1) -> None:
        with self._mutex:
            current = getattr(self, metric_name)
            setattr(self, metric_name, current + amount)

    def clear(self) -> None:
        with self._mutex:
            self.hits = 0
            self.misses = 0
            self.reads = 0
            self.writes = 0
            self.dirty_count = 0
            self.flush_count = 0
            self.flushed_total = 0

    def get_snapshot(self) -> dict:
        with self._mutex:
            h, m = self.hits, self.misses
            total_cache_ops = h + m
            hit_pct = (h / total_cache_ops * 100.0) if total_cache_ops else 0.0
            return {
                "cache_hits": h,
                "cache_misses": m,
                "cache_hit_rate_pct": round(hit_pct, 2),
                "db_reads": self.reads,
                "db_writes": self.writes,
                "db_total": self.reads + self.writes,
                "write_back_dirty": self.dirty_count,
                "write_back_flushes": self.flush_count,
                "write_back_flushed_items": self.flushed_total,
            }


stats = MetricsCollector()
