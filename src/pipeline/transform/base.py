
from abc import ABC, abstractmethod
from typing import Any
from src.pipeline.registry.registry_handler import DataRegistry

class Transformer(ABC):
    """
    Abstract Base Class for Data Transformers.
    """
    @abstractmethod
    def transform(self, registry: DataRegistry) -> Any:
        """
        Performs transformation using data from the registry.
        Can return a new DataFrame/DynamicFrame or update the registry directly.
        """
        pass
