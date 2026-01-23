from enum import Enum

from pipeline.constants.extract_helpers import ReadFormat

from pipeline.interfaces.registry_definitions import DatasetKey, SourceTable


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
    CLIENT_PREPARED = DatasetKey(alias="client_prepared")
    SALES_PREPARED = DatasetKey(alias="sales_prepared")
    
    SALES_ENRICHED = DatasetKey(alias="sales_enriched")
    ENHANCED_CLIENT = DatasetKey(alias="enhanced_client")
    
    UNIFIED_DATA = DatasetKey(alias="unified_data")