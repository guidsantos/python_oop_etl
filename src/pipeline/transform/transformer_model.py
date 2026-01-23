from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Transformer(ABC):
    """
    Abstract Base Class for Data Transformers.
    """
    @abstractmethod
    def transform(self) -> DataFrame:
        """
        Performs transformation and returns a transformed DataFrame.
        Transformers retrieve data from the global registry and return results.
        """
        pass
