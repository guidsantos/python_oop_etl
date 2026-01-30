from pyspark.sql import DataFrame

from pipeline.models.transform_model import Transformer
from src.global_variables.registry.registry_handler import DatasetRegistry
from global_variables.constants.datasets_catalog import Catalog
from src.global_variables import logger


class SalesClientJoinTransformer(Transformer):
    """
    Joins enriched sales with enriched clients.
    Performs left join to keep all sales records.
    """
    
    def transform(self, registry: DatasetRegistry) -> DataFrame:
        logger.info("Running SalesClientJoinTransformer...")
        
        # Get enriched datasets from registry
        # Using ENCHANCED_CLIENT from catalog (note: typo in catalog, but keeping consistent)
        enriched_sales = registry.get(Catalog.SALES_ENRICHED.value)
        enriched_client = registry.get(Catalog.ENHANCED_CLIENT.value)
        
        # Left join sales with clients on client_id
        unified = enriched_sales.join(
            enriched_client,
            on="client_id",
            how="left"
        ).drop(enriched_client["client_id"])
        
        logger.info(f"Sales-Client join complete. Records: {unified.count()}")
        
        return unified
