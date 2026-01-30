""""
    Create global runtime variables
"""
from global_variables.registry_instance.registry_handler import DatasetRegistry
from src.global_variables.setups import JobArgsSetup, LoggerSetup, SparkSetup, GlueSetup


# Configure local hardcoded arguments for development
local_config = {
        "env": "local",
        "database": "dev_db",
        "region": "us-east-1"
}
    
# Set force_local=True to use hardcoded args, False to use AWS Glue args
job_args = JobArgsSetup(force_local=True, local_args=local_config)

logger = LoggerSetup().get_logger()
    
spark = SparkSetup().get_spark_session()

glue = GlueSetup(spark, job_args)

registry = DatasetRegistry(logger)

logger.info("Runtime executed with success")
