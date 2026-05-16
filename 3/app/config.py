import os

STRATEGY = os.getenv("CACHE_STRATEGY", "cache_aside")
DB_URL = os.getenv("DATABASE_URL", "postgresql://bench:bench@localhost:5433/bench")
REDIS_URI = os.getenv("REDIS_URL", "redis://localhost:6380/0")
FLUSH_INTERVAL = float(os.getenv("WRITE_BACK_FLUSH_INTERVAL_SEC", "2"))
BATCH_SIZE = int(os.getenv("WRITE_BACK_FLUSH_BATCH_SIZE", "50"))
TTL = int(os.getenv("CACHE_TTL_SEC", "300"))
