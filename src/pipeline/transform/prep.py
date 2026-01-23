
from .base import Transformer
from src.pipeline.registry.registry_handler import DataRegistry
from src.boilerplate.setup.logger_setup import LoggerSetup as Logger

class CustomerPrep(Transformer):
    def transform(self, registry: DataRegistry):
        logger = Logger().get_logger()
        logger.info("Running CustomerPrep...")
        
        # Example logic: Access raw customers, clean email, standardize names
        # raw_customers = registry.get(DataKey.CUSTOMERS)
        # cleaned_customers = ... logic ...
        # return cleaned_customers
        
        # Mock implementation
        logger.info("CustomerPrep complete.")
        return "cleaned_customer_dataframe"

class OrderPrep(Transformer):
    def transform(self, registry: DataRegistry):
        logger = Logger().get_logger()
        logger.info("Running OrderPrep...")
        
        # Mock implementation
        logger.info("OrderPrep complete.")
        return "cleaned_order_dataframe"
