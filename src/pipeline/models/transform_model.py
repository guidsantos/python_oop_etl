
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Transformer(ABC):
    """
    Abstract Base Class for Data Transformers.
    """
    @abstractmethod
    def transform(self) -> DataFrame:
        """
        Performs transformation using data from the registry_instance.
        Returns a transformed DataFrame.
        """
        pass
