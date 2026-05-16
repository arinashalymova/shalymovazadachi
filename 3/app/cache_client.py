import json
from typing import Optional
import redis.asyncio as redis
from config import REDIS_URI, TTL
from metrics import stats


class RedisCache:
    def __init__(self) -> None:
        self.conn: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self.conn = redis.from_url(REDIS_URI, decode_responses=True)

    async def close(self) -> None:
        if self.conn:
            await self.conn.aclose()

    def _build_key(self, item_id: int) -> str:
        return f"item:{item_id}"

    async def fetch_raw(self, item_id: int) -> Optional[dict]:
        assert self.conn is not None
        data = await self.conn.get(self._build_key(item_id))
        return json.loads(data) if data else None

    async def fetch(self, item_id: int) -> Optional[dict]:
        result = await self.fetch_raw(item_id)
        if result is None:
            stats.increment("misses")
            return None
        stats.increment("hits")
        return result

    async def store(self, item_id: int, data: dict, is_dirty: bool = False) -> None:
        assert self.conn is not None
        data = {**data, "dirty": is_dirty}
        await self.conn.setex(
            self._build_key(item_id), TTL, json.dumps(data, default=str)
        )

    async def remove(self, item_id: int) -> None:
        assert self.conn is not None
        await self.conn.delete(self._build_key(item_id))

    async def set_dirty(self, item_id: int, data: dict) -> None:
        await self.store(item_id, data, is_dirty=True)
        stats.increment("dirty_count")

    async def scan_dirty_items(self, max_items: int) -> list[int]:
        assert self.conn is not None
        dirty_list: list[int] = []
        async for k in self.conn.scan_iter(match="item:*", count=200):
            raw_data = await self.conn.get(k)
            if not raw_data:
                continue
            parsed = json.loads(raw_data)
            if parsed.get("dirty"):
                dirty_list.append(int(k.split(":", 1)[1]))
                if len(dirty_list) >= max_items:
                    break
        return dirty_list

    async def unmark_dirty(self, item_id: int, data: dict) -> None:
        await self.store(item_id, data, is_dirty=False)


redis_cache = RedisCache()
