from src.global_variables import logger
from pipeline.models.extractor_model import Extractor
from pyspark.sql.types import StructType, StructField, StringType
from src.global_variables import spark

class IcebergExtractor(Extractor):
    def __init__(self, database: str, table_name: str):
        self.database = database
        self.table_name = table_name

    def extract(self):
        full_table_name = f"glue_catalog.{self.database}.{self.table_name}"
        logger.info(f"Extracting from Iceberg Table: {full_table_name}")

        # PLACEHOLDER TO LOCAL ENVIROMENT
        # return spark.read.format("iceberg").load(full_table_name)

        return spark.createDataFrame(data=[("iceberg",)], schema=StructType([StructField("source", StringType(), True)]))