from global_variables.constants.datasets_catalog import Catalog
from global_variables.constants.extract_helpers import ReadFormat
from pipeline.models import SourceTable

extract_exec_contract: list[SourceTable] = [
    SourceTable(
        alias=Catalog.CLIENT.value,
        database="client",
        table_name="client_table",
        read_format=ReadFormat.HIVE
    ),
    SourceTable(
        alias=Catalog.SALES.value,
        database="sales",
        table_name="sales_table",
        read_format=ReadFormat.ICEBERG
    ),
]