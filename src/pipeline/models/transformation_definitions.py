from dataclasses import dataclass, field
from typing import Optional
from src.pipeline.models.registry_definitions import DatasetKey

@dataclass(frozen=True)
class TransformationStep:
    """
    Represents a transformation step with its dependencies.

    Attributes:
        dataset_key (DatasetKey): The output dataset of this transformation.
        drop_dependencies (list[DatasetKey]): Optional list of DatasetKey objects to drop after transformation.
    """
    dataset_key: DatasetKey
    drop_dependencies: Optional[list[DatasetKey]] = field(default=None)