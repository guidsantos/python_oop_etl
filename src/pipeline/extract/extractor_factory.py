from .extractors import GlueTableExtractor, IcebergExtractor, JsonExtractor
from src.boilerplate.runtime import logger, job_args
from src.pipeline.constants.extract_helpers import ReadFormat
from src.pipeline.models.registry_definitions import SourceTable

class ExtractorFactory:
    """
    Factory to create Extractor instances.
    """
    @staticmethod
    def get_extractor(source: SourceTable):
        """
        :param source: SourceTable object definition.
        """
        logger.info(f"Getting extractor for {source.database}.{source.table_name}")
        # 1. Local execution
        if job_args.get("env") == 'local':
            logger.info(f"Local execution: {source.database}.{source.table_name}")
            path = f"../local_dev/data/{source.database}/{source.table_name}.json"
            return JsonExtractor(path)

        # 2. Check read_format for Iceberg
        if source.read_format.value == ReadFormat.ICEBERG.value:
            logger.info(f"Iceberg execution: {source.database}.{source.table_name}")
            return IcebergExtractor(
                database=source.database,
                table_name=source.table_name
            )
        
        # 3. Default to Glue Context (Hive/Default)
        logger.info(f"Glue execution: {source.database}.{source.table_name}")
        return GlueTableExtractor(
            database=source.database,
            table_name=source.table_name
        )
