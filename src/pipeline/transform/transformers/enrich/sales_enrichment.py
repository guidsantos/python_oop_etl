from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

from pipeline.models.transform_model import Transformer
from src.global_variables import registry
from global_variables.constants.datasets_catalog import Catalog
from src.global_variables import logger


class SalesEnrichmentTransformer(Transformer):
    """
    Enriches prepared sales data with calculated fields:
    - revenue_category: Categorizes sales by amount (high/medium/low)
    """
    
    def transform(self) -> DataFrame:
        logger.info("Running SalesEnrichmentTransformer...")
        
        # Get prepared sales data from registry_instance
        prepared_sales = registry.get(Catalog.SALES_PREPARED.value)
        
        # Add revenue category based on amount
        enriched = prepared_sales \
            .withColumn("revenue_category",
                       when(col("amount") >= 5000, "high")
                       .when(col("amount") >= 2000, "medium")
                       .otherwise("low"))
        
        logger.info(f"Sales enrichment complete. Records: {enriched.count()}")
        
        return enriched
