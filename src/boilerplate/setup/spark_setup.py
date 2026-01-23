
from pyspark.sql import SparkSession

from .logger_setup import LoggerSetup

logger = LoggerSetup().get_logger()


class SparkSetup:
    """
    Manages Spark Session creation and configuration.
    """
    def get_spark_session(self, app_name: str = "ETL_Pipeline", iceberg_enabled: bool = False) -> SparkSession:
        logger.info(f"Creating Spark Session: {app_name}")

        builder = SparkSession.builder.appName(app_name)

        if iceberg_enabled:
            # Example Iceberg Configuration
            # You might want to make these configurable via JobArgs in a real scenario
            builder = (builder
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-warehouse-bucket/")
                .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
                .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            )

        spark = builder.getOrCreate()
        logger.info("Spark Session created successfully.")
        return spark
