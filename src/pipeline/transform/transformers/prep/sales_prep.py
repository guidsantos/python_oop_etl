from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col

from src.pipeline.transform.transformer_model import Transformer
from src.pipeline.registry import registry
from src.pipeline.constants.dataframes_catalog import Catalog
from src.boilerplate.runtime import logger


class SalesPrepTransformer(Transformer):
    """
    Prepares raw sales data:
    - Converts timestamp strings to proper timestamps
    - Filters out inactive sales
    """
    
    def transform(self) -> DataFrame:
        logger.info("Running SalesPrepTransformer...")
        
        # Get raw sales data from registry
        raw_sales = registry.get(Catalog.SALES.value)
        
        # Convert string timestamp to timestamp type
        prepared = raw_sales \
            .withColumn("sale_date", to_timestamp(col("sale_date"), "yyyy-MM-dd HH:mm:ss"))
        
        # Filter only active sales
        prepared = prepared.filter(col("status") == "active")
        
        logger.info(f"Sales prep complete. Active sales: {prepared.count()}")
        
        return prepared
