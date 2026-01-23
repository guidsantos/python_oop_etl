
from .base import Transformer
from src.pipeline.registry.registry_handler import DataRegistry
from src.boilerplate.setup.logger_setup import LoggerSetup as Logger

class OrderEnrichment(Transformer):
    def transform(self, registry: DataRegistry):
        logger = Logger().get_logger()
        logger.info("Running OrderEnrichment...")
        
        # Example logic: Join cleaned orders with cleaned customers
        # customers = registry.get(DataKey.CUSTOMERS)
        # orders = registry.get(DataKey.ORDERS)
        # enriched = orders.join(customers, "customer_id")
        
        # Mock implementation
        logger.info("OrderEnrichment complete.")
        return "enriched_order_dataframe"
