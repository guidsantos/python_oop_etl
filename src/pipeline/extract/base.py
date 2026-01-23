
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class Extractor(ABC):
    """
    Abstract Base Class for Data Extractors.
    """
    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extracts data from the source and returns a DataFrame/DynamicFrame.
        """
        pass
