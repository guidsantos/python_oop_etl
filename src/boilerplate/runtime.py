""""
    Create global runtime variables
"""

from .setup import JobArgsSetup, LoggerSetup, SparkSetup, GlueSetup


def configure_runtime():
    # Configure local hardcoded arguments for development
    local_config = {
        "env": "local",
        "database": "dev_db",
        "region": "us-east-1"
    }
    
    # Set force_local=True to use hardcoded args, False to use AWS Glue args
    job_args_setup = JobArgsSetup(force_local=True, local_args=local_config)

    log = LoggerSetup().get_logger()
    
    spark = SparkSetup().get_spark_session()

    glue = GlueSetup(spark, job_args_setup)
    
    # Return the dictionary directly for easier access
    return log, spark, glue, job_args_setup.args

# Initialize at module level to make them available for import
logger, spark, glue, job_args = configure_runtime()

logger.info("Runtime executed with success")
