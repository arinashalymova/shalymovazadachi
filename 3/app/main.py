import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from cache_client import redis_cache
from config import STRATEGY
from database import postgres
from metrics import stats
from strategies import get_strategy
from strategies.write_back import DeferredWriteStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("app")

active_strategy = get_strategy()


@asynccontextmanager
async def app_lifespan(_: FastAPI):
    await postgres.connect()
    await redis_cache.connect()
    logger.info("Application started with strategy: %s", STRATEGY)
    if isinstance(active_strategy, DeferredWriteStrategy):
        active_strategy.start_flush_worker()
    yield
    if isinstance(active_strategy, DeferredWriteStrategy):
        await active_strategy.stop_flush_worker()
        await active_strategy.flush_data()
    await redis_cache.close()
    await postgres.close()


app = FastAPI(title="Cache Comparison App", lifespan=app_lifespan)


class UpdateItem(BaseModel):
    value: str = Field(min_length=1, max_length=256)


@app.get("/health")
async def check_health():
    return {"status": "ok", "strategy": STRATEGY}


@app.post("/admin/reset-metrics")
async def reset_all_metrics():
    stats.clear()
    return {"status": "metrics_reset"}


@app.post("/admin/flush")
async def trigger_flush():
    return await active_strategy.flush_data()


@app.get("/metrics")
async def fetch_metrics():
    snapshot = stats.get_snapshot()
    snapshot["strategy"] = STRATEGY
    return snapshot


@app.get("/items/{item_id}")
async def fetch_item(item_id: int):
    item: Optional[dict] = await active_strategy.read_item(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="not found")
    return item


@app.put("/items/{item_id}")
async def update_item(item_id: int, body: UpdateItem):
    return await active_strategy.write_item(item_id, body.value)
