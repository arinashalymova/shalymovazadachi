from typing import Optional
import asyncpg
from config import DB_URL
from metrics import stats


class PostgresClient:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def fetch_item(self, item_id: int) -> Optional[dict]:
        assert self.pool is not None
        stats.increment("reads")
        record = await self.pool.fetchrow(
            "SELECT id, value, updated_at FROM items WHERE id = $1", item_id
        )
        if not record:
            return None
        return {
            "id": record["id"],
            "value": record["value"],
            "updated_at": record["updated_at"].isoformat(),
        }

    async def save_item(self, item_id: int, item_value: str) -> dict:
        assert self.pool is not None
        stats.increment("writes")
        record = await self.pool.fetchrow(
            """
            INSERT INTO items (id, value, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (id) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW()
            RETURNING id, value, updated_at
            """,
            item_id,
            item_value,
        )
        return {
            "id": record["id"],
            "value": record["value"],
            "updated_at": record["updated_at"].isoformat(),
        }


postgres = PostgresClient()
