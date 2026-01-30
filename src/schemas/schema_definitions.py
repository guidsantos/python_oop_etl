from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType, \
    BooleanType
from global_variables.constants.datasets_catalog import Catalog


class SchemaRegistry:
    """
    Central Repository for DataFrame Schemas.
    Maps Catalog aliases to PySpark StructTypes.
    """

    _SCHEMAS = {
        # Raw Data Schemas (Extract Validation)
        Catalog.CLIENT.value: StructType([
            StructField("client_id", LongType(), True),
            StructField("client_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_at", StringType(), True),  # Raw is often string
            StructField("last_updated", StringType(), True),
            StructField("status", StringType(), True)
        ]),

        Catalog.SALES.value: StructType([
            StructField("sale_id", LongType(), True),
            StructField("client_id", LongType(), True),
            StructField("product", StringType(), True),
            StructField("quantity", LongType(), True),
            StructField("amount", DoubleType(), True),
            StructField("sale_date", StringType(), True),
            StructField("status", StringType(), True)
        ]),

        # Final Output Schema (Load Validation)
        Catalog.UNIFIED_DATA.value: StructType([
            StructField("sale_id", LongType(), True),
            StructField("product", StringType(), True),
            StructField("quantity", LongType(), True),
            StructField("amount", DoubleType(), True),
            StructField("sale_date", TimestampType(), True),
            StructField("revenue_category", StringType(), True),
            StructField("client_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("account_age_days", IntegerType(), True),
            StructField("is_recently_updated", BooleanType(), True)  # Boolean processed
        ])
    }

    @classmethod
    def get_schema(cls, alias: str) -> StructType:
        """Retrieves the schema for a given dataset alias."""
        return cls._SCHEMAS.get(alias)