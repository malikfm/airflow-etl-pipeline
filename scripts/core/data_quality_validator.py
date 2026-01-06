from datetime import UTC, datetime
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration


class DataQualityValidator:
    """Data quality validator using Great Expectations."""
    def __init__(self):
        self.context = gx.get_context()

    def validate_parquet_file(
        self,
        table_name: str,
        file_path: Path,
        rename_on_failure: bool = True,
    ) -> dict:
        """Validate a Parquet file against expectations using GX.
        
        Args:
            table_name: Name of the table to validate
            file_path: Path to Parquet file
            rename_on_failure: If True, rename invalid files to preserve evidence
            
        Returns:
            Validation results dictionary with keys:
            - success: Whether validation passed
            - statistics: Validation statistics
            - results: Detailed validation results
            - file_path: Original file path
            - renamed_to: Path file was renamed to (if validation failed and rename_on_failure=True)
        """
        # Read parquet file
        df = pd.read_parquet(file_path)

        # Add pandas data source
        data_source = self.context.data_sources.add_pandas(f"{table_name}_data_source")

        # Add dataframe asset
        data_asset = data_source.add_dataframe_asset(name=f"{table_name}_data_asset")

        # Build batch definition
        batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{table_name}_batch")

        # Link the batch to the dataframe
        batch_parameters = {"dataframe": df}
        validator = batch_definition.get_batch(batch_parameters)

        # Get expectation
        match table_name:
            case "users":
                expectations = self.get_users_expectations()
            case "products":
                expectations = self.get_products_expectations()
            case "orders":
                expectations = self.get_order_expectations()
            case "order_items":
                expectations = self.get_order_items_expectations()
            case _:
                err_msg = f"Unknown context name: {table_name}"
                raise ValueError(err_msg)

        suite = gx.ExpectationSuite(f"{table_name}_suite")
        suite.add_expectation_configurations(expectations)

        # Run validation
        results = validator.validate(suite)

        result_dict = {
            "success": results.success,
            "statistics": results.statistics,
            "results": results.results,
            "file_path": str(file_path),
        }

        # Rename invalid file if validation failed
        if not results.success and rename_on_failure:
            renamed_path = self.rename_invalid_file(file_path)
            result_dict["renamed_to"] = str(renamed_path)

        return result_dict

    def rename_invalid_file(self, file_path: Path) -> Path:
        """Rename an invalid file to preserve evidence.
        
        Format: {original_name}.invalid.{datetime_execution}
        Where datetime_execution is in YYYYMMDD_HHmmss format (UTC).
        
        Example:
            ./data/orders/2025-01-01.parquet
            -> ./data/orders/2025-01-01.parquet.invalid.20250102_000130
        
        Args:
            file_path: Path to the invalid file
            
        Returns:
            Path to the renamed file
        """
        file_path = Path(file_path)

        # Generate timestamp in YYYYMMDD_HHmmss format (UTC)
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

        # Create new filename: {original}.invalid.{timestamp}
        new_filename = f"{file_path.name}.invalid.{timestamp}"
        new_path = file_path.parent / new_filename

        # Rename the file
        file_path.rename(new_path)

        return new_path

    def get_users_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for users Parquet file.
        
        Expectations:
        - File is not empty
        - email is unique and not null
        - id is unique
        
        Returns:
            List of expectations
        """
        return [
            ExpectationConfiguration(
                type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_unique",
                kwargs={"column": "id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_unique",
                kwargs={"column": "email"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "email"}
            )
        ]

    def get_products_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for products Parquet file.
        
        Expectations:
        - File is not empty
        - price is positive
        - id is unique
        
        Returns:
            List of expectations
        """
        return [
            ExpectationConfiguration(
                type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_unique",
                kwargs={"column": "id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_between",
                kwargs={"column": "price", "min_value": 0, "strict_min": True}
            )
        ]

    def get_order_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for orders Parquet file.
        
        Expectations:
        - File is not empty
        - user_id column exists
        - user_id is not null
        - id column is unique
        
        Returns:
            List of expectations
        """
        return [
            ExpectationConfiguration(
                type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1}
            ),
            ExpectationConfiguration(
                type="expect_column_to_exist",
                kwargs={"column": "user_id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "user_id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_unique",
                kwargs={"column": "id"}
            )
        ]

    def get_order_items_expectations(self) -> list[ExpectationConfiguration]:
        """Get expectations for order_items Parquet file.
        
        Expectations:
        - File is not empty (if orders exist)
        - quantity is positive
        - order_id and product_id are not null
        
        Returns:
            List of expectations
        """
        return [
            ExpectationConfiguration(
                type="expect_column_to_exist",
                kwargs={"column": "order_id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "order_id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_not_be_null",
                kwargs={"column": "product_id"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_between",
                kwargs={"column": "quantity", "min_value": 1}
            )
        ]
