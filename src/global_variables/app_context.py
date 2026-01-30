""""
    Create global runtime variables
"""

from src.global_variables.registry.registry_handler import DatasetRegistry
from src.global_variables.setups import JobArgsSetup, LoggerSetup, SparkSetup, GlueSetup


# Configure local hardcoded arguments for development
local_config = {
        "env": "local",
        "database": "dev_db",
        "region": "us-east-1"
}
    
# Set force_local=True to use hardcoded args, False to use AWS Glue args
job_args_setup = JobArgsSetup(force_local=True, local_args=local_config)

logger = LoggerSetup().get_logger()
    
spark = SparkSetup().get_spark_session()

glue = GlueSetup(spark, job_args_setup)

registry = DatasetRegistry()

logger.info("Runtime executed with success")
