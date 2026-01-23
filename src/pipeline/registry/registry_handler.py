from typing import Optional
from pyspark.sql import DataFrame
from src.pipeline.interfaces.registry_definitions import DatasetKey
from src.boilerplate.runtime import logger
import threading


class DatasetRegistry:
    """
    Singleton Registry for managing PySpark DataFrames.
    Ensures only one instance exists per runtime.
    """

    _instance: Optional['DatasetRegistry'] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls):
        """
        Controls the creation of the instance. If an instance already exists,
        return it. If not, create it.
        """
        if cls._instance is None:
            # Thread-safe check to ensure atomic creation
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatasetRegistry, cls).__new__(cls)
                    # We initialize the store here to prevent re-initialization
                    # every time DatasetRegistry() is called.
                    cls._instance._store = {}
        return cls._instance

    def register(self, key: DatasetKey, df: DataFrame) -> None:
        logger.info(f"Registering dataset: {key.alias}")
        self._store[key.alias] = df

    def get(self, key: DatasetKey) -> DataFrame:
        try:
            return self._store[key.alias]
        except KeyError:
            raise ValueError(f"Dataset '{key.alias}' not found in registry.")

    def drop(self, key: DatasetKey) -> None:
        df = self._store.pop(key.alias, None)
        if df:
            if df.is_cached:
                logger.info(f"Unpersisting cached dataset: {key.alias}")
                df.unpersist()
            logger.info(f"Dropped dataset: {key.alias}")
        else:
            logger.warning(f"Attempted to drop non-existent dataset: {key.alias}")

    @property
    def keys(self) -> list[str]:
        return list(self._store.keys())

    @property
    def store(self) -> dict[str, DataFrame]:
        return self._store


# Usability wrapper (optional, but maintains backward compatibility)
registry = DatasetRegistry()