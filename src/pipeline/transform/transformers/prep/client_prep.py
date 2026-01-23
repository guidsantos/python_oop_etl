from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, current_date, datediff

from src.pipeline.transform.transformer_model import Transformer
from src.pipeline.registry import registry
from src.pipeline.constants.datasets_catalog import Catalog
from src.boilerplate.runtime import logger


class ClientPrepTransformer(Transformer):
    """
    Prepares raw client data:
    - Converts timestamp strings to proper timestamps
    - Filters out inactive clients
    """
    
    def transform(self) -> DataFrame:
        logger.info("Running ClientPrepTransformer...")
        
        # Get raw client data from registry
        raw_client = registry.get(Catalog.CLIENT.value)
        
        # Convert string timestamps to timestamp type
        prepared = raw_client \
            .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("last_updated", to_timestamp(col("last_updated"), "yyyy-MM-dd HH:mm:ss"))
        
        # Filter only active clients
        prepared = prepared.filter(col("status") == "active")
        
        logger.info(f"Client prep complete. Active clients: {prepared.count()}")
        
        return prepared
