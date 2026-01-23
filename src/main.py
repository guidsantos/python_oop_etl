import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.pipeline.extract.batch_extrator_runner import BatchExtractionRunner
from src.boilerplate.runtime import logger
from src.pipeline.registry import registry
from src.pipeline.constants.dataframes_catalog import Catalog

EXTRACTION_SOURCES = [
    Catalog.CLIENT.value,
    Catalog.SALES.value
]

def main():
    logger.info("Starting ETL Pipeline...")

    def Orchestrator():
        BatchExtractionRunner(sources=EXTRACTION_SOURCES).run()
    #
    #     transfom().run()
    #
    #     load().run()

    Orchestrator()

    logger.info(f"DataFrames in registry: {registry.keys}")

if __name__ == "__main__":
    main()
