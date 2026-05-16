from abc import ABC, abstractmethod
from typing import Optional


class CachingStrategy(ABC):
    @abstractmethod
    async def read_item(self, item_id: int) -> Optional[dict]:
        raise NotImplementedError

    @abstractmethod
    async def write_item(self, item_id: int, value: str) -> dict:
        raise NotImplementedError

    async def flush_data(self) -> dict:
        return {"flushed": 0}
