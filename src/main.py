import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.pipeline.extract.batch_extrator_runner import BatchExtractionRunner
from src.pipeline.transform.batch_transformation_runner import BatchTransformationRunner
from src.pipeline.load.writer import Writer
from src.boilerplate.runtime import logger
from src.pipeline.registry import registry
from src.pipeline.constants.datasets_catalog import Catalog
from src.pipeline.models import TransformationStep

EXTRACTION_SOURCES = [
    Catalog.CLIENT.value,
    Catalog.SALES.value
]

TRANSFORMATION_STEPS = [
    # Prep stage: Clean and filter, drop raw data after
    TransformationStep(
        dataset_key=Catalog.CLIENT_PREPARED.value,
        drop_dependencies=[Catalog.CLIENT.value]
    ),
    TransformationStep(
        dataset_key=Catalog.SALES_PREPARED.value,
        drop_dependencies=[Catalog.SALES.value]
    ),
    
    # Enrichment stage: Add calculated fields, drop prepared data after
    TransformationStep(
        dataset_key=Catalog.ENHANCED_CLIENT.value,
        drop_dependencies=[Catalog.CLIENT_PREPARED.value]
    ),
    TransformationStep(
        dataset_key=Catalog.SALES_ENRICHED.value,
        drop_dependencies=[Catalog.SALES_PREPARED.value]
    ),
    
    # Final stage: Join tables, drop enriched data after
    TransformationStep(
        dataset_key=Catalog.UNIFIED_DATA.value,
        drop_dependencies=[Catalog.SALES_ENRICHED.value, Catalog.ENHANCED_CLIENT.value]
    ),
]

def main():
    logger.info("Starting ETL Pipeline...")

    def Orchestrator():
        # Step 1: Extract raw data
        logger.info("=" * 50)
        logger.info("EXTRACTION PHASE")
        logger.info("=" * 50)
        BatchExtractionRunner(sources=EXTRACTION_SOURCES).run()
        
        # Step 2: Transform data
        logger.info("=" * 50)
        logger.info("TRANSFORMATION PHASE")
        logger.info("=" * 50)
        BatchTransformationRunner(transformations=TRANSFORMATION_STEPS).run()
        
        # Step 3: Load/Write data
        logger.info("=" * 50)
        logger.info("LOAD PHASE")
        logger.info("=" * 50)
        Writer().write()

    Orchestrator()

    logger.info("=" * 50)
    logger.info(f"ETL Pipeline Complete!")
    logger.info(f"DataFrames in registry: {registry.keys}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
