from dataclasses import dataclass

from pipeline.contracts.transformation_contract import transformation_exec_contract
from pipeline.contracts.extract_contract import extract_exec_contract
from pipeline.contracts.load_contract import load_exec_contract
from pipeline.load.writer import LoadSet
from pipeline.models import SourceTable
from pipeline.models.registry_definitions import TransformationStep

@dataclass(frozen=True)
class PipelineContractsRegistry:
    EXTRACT: list[SourceTable]
    TRANSFORMATION: list[TransformationStep]
    LOAD: LoadSet

PipelineContracts = PipelineContractsRegistry(
    EXTRACT=extract_exec_contract,
    TRANSFORMATION=transformation_exec_contract,
    LOAD=load_exec_contract
)