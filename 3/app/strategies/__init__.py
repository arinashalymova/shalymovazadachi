from config import STRATEGY
from strategies.base import CachingStrategy
from strategies.cache_aside import LazyLoadingStrategy
from strategies.write_back import DeferredWriteStrategy
from strategies.write_through import SynchronousWriteStrategy


def get_strategy() -> CachingStrategy:
    strategy_map = {
        "cache_aside": LazyLoadingStrategy,
        "write_through": SynchronousWriteStrategy,
        "write_back": DeferredWriteStrategy,
    }
    strategy_class = strategy_map.get(STRATEGY)
    if strategy_class is None:
        raise ValueError(f"Unknown strategy: {STRATEGY}")
    return strategy_class()
