from src.global_variables import logger
from pipeline.models.extractor_model import Extractor
from pyspark.sql.types import StructType, StructField, StringType
from src.global_variables import spark

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

        return spark.createDataFrame(data=[("glue_catalog",)],
                                     schema=StructType([StructField("source", StringType(), True)]))

