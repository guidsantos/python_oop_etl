from src.pipeline.registry import registry
from src.boilerplate.runtime import logger
from src.pipeline.interfaces.registry_definitions import DatasetKey, TransformationStep
from .transformer_factory import TransformerFactory
from typing import Union


class BatchTransformationRunner:
    """
    Manages the execution of a list of transformation steps.
    
    Each transformation retrieves data from registry, transforms it,
    and registers the result back to registry.
    """

    def __init__(self, transformations: list[Union[TransformationStep, DatasetKey]]):
        """
        :param transformations: List of TransformationStep or DatasetKey objects.
                               TransformationStep includes drop_dependencies.
                               DatasetKey is converted to TransformationStep with no dependencies.
        """
        # Convert DatasetKey to TransformationStep for backward compatibility
        self._transformations = [
            t if isinstance(t, TransformationStep) else TransformationStep(dataset_key=t)
            for t in transformations
        ]

    def run(self) -> None:
        """
        Iterates through all transformations and executes them in sequence.
        """
        
        logger.info(f"Starting batch transformation for {len(self._transformations)} steps.")

        for transformation_step in self._transformations:
            self._process_single_transformation(transformation_step)

        logger.info("Batch transformation completed.")

    def _process_single_transformation(self, transformation_step: TransformationStep) -> None:
        """
        Executes a single transformation step and handles cleanup.
        """
        dataset_key = transformation_step.dataset_key
        
        try:
            logger.info(f"Processing transformation: {dataset_key.alias}")

            transformer = TransformerFactory.get_transformer(dataset_key)

            dataframe = transformer.transform()

            if dataframe is not None:
                registry.register(dataset_key, dataframe)
                logger.info(f"Saved to registry: '{dataset_key.alias}'")
                
                # Auto-cleanup dependencies if configured
                if transformation_step.drop_dependencies:
                    logger.info(f"Cleaning up dependencies for {dataset_key.alias}: {[d.alias for d in transformation_step.drop_dependencies]}")
                    for dependency_key in transformation_step.drop_dependencies:
                        try:
                            registry.drop(dependency_key)
                            logger.info(f"Dropped dependency: '{dependency_key.alias}'")
                        except Exception as drop_error:
                            logger.warning(f"Could not drop '{dependency_key.alias}': {drop_error}")
            else:
                logger.warning(f"Transformer for {dataset_key.alias} returned None. Skipping registry update.")

            logger.info(f"Successfully transformed: {dataset_key.alias}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to transform {dataset_key.alias}: {error_msg}", exc_info=True)
            raise e
