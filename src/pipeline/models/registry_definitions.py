from dataclasses import dataclass, field
from typing import Optional
from src.pipeline.constants.extract_helpers import ReadFormat

@dataclass(frozen=True)
class DatasetKey:
    """
    Represents a dataset key.

    Attributes:
        alias (str): The alias of the dataset.
    """
    alias: str

@dataclass(frozen=True)
class SourceTable(DatasetKey):
    """
    Represents a source table.
    
    Attributes:
        alias (str): The alias of the dataset.
        database (str): The database of the table.
        table_name (str): The name of the table.
        read_format (ReadFormat): The format of the table.
    """
    database: str
    table_name: str
    read_format: ReadFormat