from src.pipeline.registry import registry
from src.pipeline.constants.load_helper import FINAL_DATASET
from src.boilerplate.runtime import logger, job_args


class Writer:
    """
    Simple writer class to save the final dataset.
    Writes data from the registry using the UNIFIED_DATA catalog constant.
    """
    
    def write(self) -> None:
        """
        Retrieves the final dataset from registry and writes it to storage.
        For local execution, saves as parquet to local_dev/data/output.
        For production, can be extended to write to databases, S3, etc.
        """
        logger.info("Starting write operation...")
        
        # Get the final dataset from registry
        final_df = registry.get(FINAL_DATASET.value)
        
        # Determine output path based on environment
        if job_args.get("env") == "local":
            output_path = "../local_dev/data/output.json"
            logger.info(f"Writing to local parquet: {output_path}")
            
            # Write as parquet
            final_df.write.mode("overwrite").json(output_path)
            
            logger.info(f"Successfully wrote data to {output_path}")
        else:
            # For production: write to database, S3, etc.
            logger.info("Production write logic not yet implemented")
            # Example for production:
            # final_df.write.mode("overwrite").saveAsTable("target_database.target_table")
        
        logger.info("Write operation completed.")
