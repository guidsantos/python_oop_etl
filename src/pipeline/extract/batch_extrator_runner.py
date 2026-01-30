from src.global_variables import registry

from src.global_variables import logger
from src.pipeline.models.registry_definitions import SourceTable
from .extractor_factory import ExtractorFactory


class BatchExtractionRunner:
    """
    Manages the execution of a list of SourceTable objects.
    
    This class ensures that all sources are attempted even if one fails
    (Fault Tolerance) and provides a summary report at the end.
    """

    def __init__(self, sources: list[SourceTable]):
        """
        :param sources: A list of SourceTable objects to be processed.
        """
        self._sources = sources

    def run(self) -> None:
        """
        Iterates through all provided sources and executes their extraction logic.
        """
        
        logger.info(f"Starting batch execution for {len(self._sources)} sources.")

        for source in self._sources:
            self._process_single_source(source)

        logger.info("Batch execution completed.")

    def _process_single_source(self, source: SourceTable) -> None:
        """
        Encapsulates the logic for a single source to ensure error isolation.
        """
        try:
            logger.info(f"Processing: {source.database}.{source.table_name}")

            extractor = ExtractorFactory.get_extractor(source)

            dataframe = extractor.extract()

            if dataframe is not None:
                registry.register(source, dataframe)
                logger.info(f"Saved to registry: ['{source.alias}']")
            else:
                logger.warning(f"Extractor for {source.alias} returned None. Skipping registry update.")

            logger.info(f"Successfully extracted: {source.database}.{source.table_name}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to extract {source.database}.{source.table_name}: {error_msg}", exc_info=True)
            raise e