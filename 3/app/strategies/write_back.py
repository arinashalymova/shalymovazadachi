import asyncio
import logging
from typing import Optional
from cache_client import redis_cache
from config import BATCH_SIZE, FLUSH_INTERVAL
from database import postgres
from metrics import stats
from strategies.base import CachingStrategy

log = logging.getLogger(__name__)


class DeferredWriteStrategy(CachingStrategy):
    def __init__(self) -> None:
        self._background_task: Optional[asyncio.Task] = None

    def start_flush_worker(self) -> None:
        if self._background_task is None:
            self._background_task = asyncio.create_task(self._flush_worker())

    async def stop_flush_worker(self) -> None:
        if self._background_task:
            self._background_task.cancel()
            try:
                await self._background_task
            except asyncio.CancelledError:
                pass
            self._background_task = None

    async def read_item(self, item_id: int) -> Optional[dict]:
        cached_data = await redis_cache.fetch(item_id)
        if cached_data is not None:
            return {k: v for k, v in cached_data.items() if k != "dirty"}

        db_record = await postgres.fetch_item(item_id)
        if db_record is None:
            return None
        await redis_cache.store(item_id, db_record)
        return db_record

    async def write_item(self, item_id: int, value: str) -> dict:
        item_data = {"id": item_id, "value": value, "updated_at": None}
        await redis_cache.set_dirty(item_id, item_data)
        return {"id": item_id, "value": value}

    async def flush_data(self) -> dict:
        dirty_items = await redis_cache.scan_dirty_items(BATCH_SIZE)
        processed = 0
        for item_id in dirty_items:
            cached_entry = await redis_cache.fetch_raw(item_id)
            if cached_entry is None or not cached_entry.get("dirty"):
                continue
            db_record = await postgres.save_item(item_id, cached_entry["value"])
            await redis_cache.unmark_dirty(item_id, db_record)
            processed += 1

        if processed:
            stats.increment("flush_count")
            stats.increment("flushed_total", processed)
            log.info("Flushed %d dirty items to database", processed)

        return {"flushed": processed, "pending_scan": len(dirty_items)}

    async def _flush_worker(self) -> None:
        while True:
            try:
                await asyncio.sleep(FLUSH_INTERVAL)
                result = await self.flush_data()
                if result["flushed"]:
                    log.info(
                        "Auto-flush completed: %d items (interval: %.1fs)",
                        result["flushed"],
                        FLUSH_INTERVAL,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Flush worker error")
