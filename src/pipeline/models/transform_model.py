
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from src.global_variables.registry.registry_handler import DatasetRegistry

class Transformer(ABC):
    """
    Abstract Base Class for Data Transformers.
    """
    @abstractmethod
    def transform(self) -> DataFrame:
        """
        Performs transformation using data from the registry.
        Returns a transformed DataFrame.
        """
        pass
