from enum import Enum

from global_variables.constants.extract_helpers import ReadFormat

from pipeline.models.registry_definitions import DatasetKey, SourceTable, TransformationStep
from pipeline.transform import ClientPrepTransformer, SalesPrepTransformer, SalesEnrichmentTransformer, \
    ClientEnrichmentTransformer, SalesClientJoinTransformer


class Catalog(Enum):

    CLIENT = SourceTable(alias="client",
                        database="client",
                        table_name="client_table",
                        read_format=ReadFormat.HIVE)

    SALES = SourceTable(alias="sales",
                       database="sales",
                       table_name="sales_table",
                       read_format=ReadFormat.ICEBERG)

    # Transformation outputs (DatasetKey only, no dependencies)
    CLIENT_PREPARED = TransformationStep(alias="client_prepared", transformer_class=ClientPrepTransformer)
    SALES_PREPARED = TransformationStep(alias="sales_prepared", transformer_class=SalesPrepTransformer)
    
    SALES_ENRICHED = TransformationStep(alias="sales_enriched", transformer_class=SalesEnrichmentTransformer)
    ENHANCED_CLIENT = TransformationStep(alias="enhanced_client", transformer_class=ClientEnrichmentTransformer)
    
    UNIFIED_DATA = TransformationStep(alias="unified_data", transformer_class=SalesClientJoinTransformer)
