from pipeline.contracts.pipeline_contracts import PipelineContracts
from src.pipeline.extract.batch_extrator_runner import BatchExtractionRunner
from src.pipeline.transform.batch_transformation_runner import BatchTransformationRunner
from src.pipeline.load.writer import Writer
from src.global_variables import logger, registry

def main():
    logger.info("Starting ETL Pipeline...")

    # Step 1: Extract raw data
    logger.info("=" * 50)
    logger.info("EXTRACTION PHASE")
    logger.info("=" * 50)
    BatchExtractionRunner(sources=PipelineContracts.EXTRACT).run()
    
    # Step 2: Transform data
    logger.info("=" * 50)
    logger.info("TRANSFORMATION PHASE")
    logger.info("=" * 50)
    BatchTransformationRunner(transformations=PipelineContracts.TRANSFORMATION).run()
    
    # Step 3: Load/Write data
    logger.info("=" * 50)
    logger.info("LOAD PHASE")
    logger.info("=" * 50)
    Writer(PipelineContracts.LOAD).write()

    logger.info("=" * 50)
    logger.info(f"ETL Pipeline Complete!")
    logger.info(f"DataFrames in registry: {registry.keys}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()
