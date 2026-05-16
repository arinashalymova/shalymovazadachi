from typing import Optional
from cache_client import redis_cache
from database import postgres
from strategies.base import CachingStrategy


class LazyLoadingStrategy(CachingStrategy):
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
        updated_record = await postgres.save_item(item_id, value)
        await redis_cache.remove(item_id)
        return updated_record
