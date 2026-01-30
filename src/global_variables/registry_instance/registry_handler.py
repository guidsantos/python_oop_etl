from typing import Optional
from pyspark.sql import DataFrame
from models.registry_definitions import DatasetKey


class DatasetRegistry:
    """
    Singleton Registry for managing PySpark DataFrames.
    Ensures only one instance exists per runtime.
    """

    def __init__(self, logger) -> None:
        self._store = {}
        self.logger = logger

    def register(self, key: DatasetKey, df: DataFrame) -> None:
        self.logger.info(f"Registering dataset: {key.alias}")
        self._store[key.alias] = df

    def get(self, key: str) -> DataFrame:
        try:
            return self._store[key]
        except KeyError:
            raise ValueError(f"Dataset '{key}' not found in registry_instance.")

    def drop(self, key: str) -> None:
        # 1. Atomic Pop: Try to remove key and retrieve value in one step.
        # If key doesn't exist, it returns None (no error raised).
        df: Optional[DataFrame] = self._store.pop(key, None)

        # 2. Handle "Not Found" case first (Guard Clause)
        if df is None:
            self.logger.warning(f"Attempted to drop non-existent dataset: {key}")
            return

        # 3. Handle "Found" case (Spark Cleanup)
        if df.is_cached:
            self.logger.info(f"Unpersisting cached dataset: {key}")
            df.unpersist()

        self.logger.info(f"Dropped dataset: {key}")

    @property
    def keys(self) -> list[str]:
        return list(self._store.keys())

    @property
    def store(self) -> dict[str, DataFrame]:
        return self._store