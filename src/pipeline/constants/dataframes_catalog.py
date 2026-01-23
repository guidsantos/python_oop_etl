from enum import Enum

from pipeline.constants.extract import ReadFormat

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

    CLIENT_PREPARED =  DatasetKey(alias="client_prepared")
    SALES_PREPARED =  DatasetKey(alias="sales_prepared")

    ENCHANCED_CLIENT = DatasetKey(alias="enchanced_client")
    UNIFIED_DATA = DatasetKey(alias="unified_data")

    FINAL_DATA = DatasetKey(alias="final_data")