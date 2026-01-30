from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DataType, IntegerType, LongType, DoubleType, DecimalType, TimestampType
from src.global_variables import logger


class ValidationException(Exception):
    """Custom exception for schema validation failures."""
    pass


class DataValidator:
    """
    Service responsible for validating DataFrames against defined Schemas.
    Uses native PySpark Type Objects for robust comparison (No strings attached).
    """

    def validate(self, df: DataFrame, schema: StructType, dataset_alias: str) -> None:
        """
        Validates column existence and types using Spark's native objects.
        """
        if not schema:
            logger.warning(f"No schema defined for '{dataset_alias}'. Skipping validation.")
            return

        logger.info(f"Validating schema for: {dataset_alias}")

        # --- 1. Validate Column Existence ---
        # Get set of column names
        expected_cols = set(field.name for field in schema.fields)
        actual_cols = set(df.columns)

        missing_cols = expected_cols - actual_cols
        if missing_cols:
            error_msg = f"Missing columns in '{dataset_alias}': {missing_cols}"
            logger.error(error_msg)
            raise ValidationException(error_msg)

        # --- 2. Validate Data Types (Object Comparison) ---
        # Create a map of {column_name: DataTypeObject} for the actual DataFrame
        # df.schema behaves like a list of StructFields, so we can iterate it directly
        df_type_map = {field.name: field.dataType for field in df.schema}

        type_mismatches = []

        for field in schema.fields:
            col_name = field.name

            # Skip if column is missing (already handled above)
            if col_name not in df_type_map:
                continue

            expected_type = field.dataType
            actual_type = df_type_map[col_name]

            # Compare the actual Objects, not strings
            if not isinstance(actual_type, type(expected_type)):
                # Use simpleString() ONLY for the error message, to make it readable
                type_mismatches.append(
                    f"Column '{col_name}': expected {type(expected_type).__name__}, got {type(actual_type).__name__}"
                )

        if type_mismatches:
            error_msg = f"Type Validation Failed for '{dataset_alias}'. Mismatches: {type_mismatches}"
            logger.error(error_msg)
            raise ValidationException(error_msg)

        logger.info(f"Schema validation passed for: {dataset_alias}")