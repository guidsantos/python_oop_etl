from .transformers import (
    ClientPrepTransformer,
    SalesPrepTransformer,
    ClientEnrichmentTransformer,
    SalesEnrichmentTransformer,
    SalesClientJoinTransformer
)
from src.pipeline.constants.dataframes_catalog import Catalog
from src.boilerplate.runtime import logger


class TransformerFactory:
    """
    Factory to map DatasetKey to appropriate Transformer instances.
    """
    
    # Map output dataset keys to their transformer classes
    _transformers = {
        Catalog.CLIENT_PREPARED.value.alias: ClientPrepTransformer,
        Catalog.SALES_PREPARED.value.alias: SalesPrepTransformer,
        Catalog.ENHANCED_CLIENT.value.alias: ClientEnrichmentTransformer,
        Catalog.SALES_ENRICHED.value.alias: SalesEnrichmentTransformer,
        Catalog.UNIFIED_DATA.value.alias: SalesClientJoinTransformer,
    }
    
    @staticmethod
    def get_transformer(dataset_key):
        """
        Get transformer instance for a given dataset key.
        
        :param dataset_key: DatasetKey object or alias string
        :return: Transformer instance
        """
        # Handle both DatasetKey objects and string aliases
        alias = dataset_key.alias if hasattr(dataset_key, 'alias') else dataset_key
        
        transformer_class = TransformerFactory._transformers.get(alias)
        
        if not transformer_class:
            logger.error(f"Transformer not found for: {alias}")
            raise ValueError(f"Transformer not found for: {alias}")
        
        logger.info(f"Creating transformer for: {alias}")
        return transformer_class()
