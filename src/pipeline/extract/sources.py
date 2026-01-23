
from pyspark.sql.types import StructType, StructField, StringType
from .base import Extractor
from src.boilerplate.runtime import logger, glue, spark

class GlueTableExtractor(Extractor):
    def __init__(self, database: str, table_name: str):
        self.database = database
        self.table_name = table_name

    def extract(self):
        logger.info(f"Extracting from Glue Table: {self.database}.{self.table_name}")
        
        # PLACEHOLDER TO LOCAL ENVIROMENT
        # return glue.glue_context.create_dynamic_frame.from_catalog(
        #     database=self.database,
        #     table_name=self.table_name
        # ).toDF()

        return spark.createDataFrame(data=[("glue_catalog",)], schema=StructType([StructField("source", StringType(), True)]))

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

class JsonExtractor(Extractor):
    def __init__(self, path: str):
        self.path = path

    def extract(self):
        logger.info(f"Extracting from JSON file (Local): {self.path}")
        return spark.read.json(self.path)
