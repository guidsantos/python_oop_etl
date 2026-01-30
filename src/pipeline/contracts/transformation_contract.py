from global_variables.constants.datasets_catalog import Catalog
from models import TransformationStep
from pipeline.transform import SalesClientJoinTransformer, ClientEnrichmentTransformer, SalesEnrichmentTransformer, \
    SalesPrepTransformer, ClientPrepTransformer

transformation_exec_contract = [
    TransformationStep(
        alias=Catalog.CLIENT_PREPARED.value,
        transformer_class=ClientPrepTransformer
    ),
    TransformationStep(
        alias=Catalog.SALES_PREPARED.value,
        transformer_class=SalesPrepTransformer
    ),
    TransformationStep(
        alias=Catalog.SALES_ENRICHED.value,
        transformer_class=SalesEnrichmentTransformer
    ),
    TransformationStep(
        alias=Catalog.ENHANCED_CLIENT.value,
        transformer_class=ClientEnrichmentTransformer
    ),
    TransformationStep(
        alias=Catalog.UNIFIED_DATA.value,
        transformer_class=SalesClientJoinTransformer
    ),
]