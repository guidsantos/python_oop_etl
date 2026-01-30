from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, datediff, when

from models.transform_model import Transformer
from src.global_variables import registry
from global_variables.constants.datasets_catalog import Catalog
from src.global_variables import logger


class ClientEnrichmentTransformer(Transformer):
    """
    Enriches prepared client data with calculated fields:
    - account_age_days: Days since account creation
    - is_recently_updated: Boolean if updated in last 30 days
    """
    
    def transform(self) -> DataFrame:
        logger.info("Running ClientEnrichmentTransformer...")
        
        # Get prepared client data from registry_instance
        prepared_client = registry.get(Catalog.CLIENT_PREPARED.value)
        
        # Add calculated fields
        enriched = prepared_client \
            .withColumn("account_age_days", datediff(current_date(), col("created_at"))) \
            .withColumn("is_recently_updated", 
                       when(datediff(current_date(), col("last_updated")) <= 30, True)
                       .otherwise(False))
        
        logger.info(f"Client enrichment complete. Records: {enriched.count()}")
        
        return enriched
