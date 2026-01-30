from dataclasses import dataclass

from pipeline.validator.data_validator import DataValidator
from schemas.schema_definitions import SchemaRegistry
from src.global_variables import registry
from global_variables.constants.load_helper import FINAL_DATASET
from src.global_variables import logger, job_args


@dataclass(frozen=True)
class LoadSet:
    full_table_name: str
    dataset: str

class Writer:
    """
    Simple writer class to save the final dataset.
    Writes data from the registry_instance using the UNIFIED_DATA catalog constant.
    """

    def __init__(self, source: LoadSet):
        self._validator = DataValidator()
        self.source = source
    
    def write(self) -> None:
        """
        Retrieves the final dataset from registry_instance and writes it to storage.
        For local execution, saves as parquet to local_dev/data/output.
        For production, can be extended to write to databases, S3, etc.
        """
        logger.info("Starting write operation...")
        
        # Get the final dataset from registry_instance
        final_df = registry.get(self.source.dataset)

        schema = SchemaRegistry.get_schema(self.source.full_table_name)  # ou self.source.dataset
        self._validator.validate(final_df, schema, self.source.dataset)
        
        # Determine output path based on environment
        if job_args.get("env") == "local":
            #TODO: Implement write to local file

            logger.info(f"Successfully wrote data to {final_df}")
        else:
            # For production: write to database, S3, etc.
            logger.info("Production write logic not yet implemented")
            # Example for production:
            # final_df.write.mode("overwrite").saveAsTable("target_database.target_table")
        
        logger.info("Write operation completed.")
