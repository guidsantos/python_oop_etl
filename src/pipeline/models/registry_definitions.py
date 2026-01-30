from dataclasses import dataclass, field
from typing import Optional

from global_variables.constants.extract_helpers import ReadFormat
from pipeline.models.transform_model import Transformer


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

@dataclass(frozen=True)
class TransformationStep(DatasetKey):
    """
    Represents a transformation step with its dependencies.

    Attributes:
        dataset_key (DatasetKey): The output dataset of this transformation.
        drop_dependencies (list[DatasetKey]): Optional list of DatasetKey objects to drop after transformation.
    """
    dataset_key: DatasetKey
    transformer_class: type[Transformer]
    drop_dependencies: Optional[list[DatasetKey]] = field(default=list)