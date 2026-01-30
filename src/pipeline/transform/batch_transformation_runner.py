from src.global_variables import logger, registry
from models import TransformationStep


class BatchTransformationRunner:
    """
    Manages the execution of a list of transformation steps.
    
    Each transformation retrieves data from registry_instance, transforms it,
    and registers the result back to registry_instance.
    """

    def __init__(self, transformations: list[TransformationStep]):
        """
        :param transformations: List of TransformationStep or DatasetKey objects.
                               TransformationStep includes drop_dependencies.
                               DatasetKey is converted to TransformationStep with no dependencies.
        """
        # Convert DatasetKey to TransformationStep for backward compatibility
        self._transformations = transformations

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
        try:
            dataset_key = transformation_step.alias

            logger.info(f"Processing transformation: {dataset_key}")

            dataframe = transformation_step.transformer_class().transform()

            if dataframe is not None:
                registry.register(transformation_step, dataframe)
                logger.info(f"Saved to registry_instance: '{dataset_key}'")

                logger.info(f"Cleaning up dependencies for {dataset_key}: {transformation_step.drop_dependencies}")
                for dependency_key in transformation_step.drop_dependencies:
                    registry.drop(dependency_key)
                    logger.info(f"Dropped dependency: '{dependency_key.alias}'")

            logger.info(f"Successfully transformed: {dataset_key}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to transform {dataset_key}: {error_msg}", exc_info=True)
            raise e
