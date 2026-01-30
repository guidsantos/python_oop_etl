from src.global_variables import logger
from pipeline.models.extractor_model import Extractor
from src.global_variables import spark

class JsonExtractor(Extractor):
    def __init__(self, path: str):
        self.path = path

    def extract(self):
        logger.info(f"Extracting from JSON file (Local): {self.path}")
        # Read JSON with multiLine option for JSON arrays and cache to avoid corrupt record issues
        df = spark.read.option("multiLine", "true").json(self.path)
        return df
