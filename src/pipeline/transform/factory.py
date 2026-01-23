from .prep import CustomerPrep, OrderPrep
from .enrich import OrderEnrichment
from src.boilerplate.setup.logger_setup import LoggerSetup as Logger

class TransformerFactory:
    """
    Factory to retrieve transformers by name.
    """
    _transformers = {
        "customer_prep": CustomerPrep,
        "order_prep": OrderPrep,
        "order_enrichment": OrderEnrichment
    }

    @staticmethod
    def get_transformer(name: str):
        logger = Logger().get_logger()
        transformer_class = TransformerFactory._transformers.get(name)
        
        if not transformer_class:
            logger.error(f"Transformer not found: {name}")
            raise ValueError(f"Transformer not found: {name}")
        
        return transformer_class()
