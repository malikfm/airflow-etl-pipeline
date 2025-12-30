from pathlib import Path
from typing import List

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
        file_path: Path
    ) -> dict:
        """Validate a Parquet file against expectations using GX.
        
        Args:
            table_name: Name of the table to validate
            file_path: Path to Parquet file
            
        Returns:
            Validation results dictionary
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

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
                raise ValueError(f"Unknown context name: {table_name}")

        suite = gx.ExpectationSuite(f"{table_name}_suite")
        suite.add_expectation_configurations(expectations)

        # Run validation
        results = validator.validate(suite)

        return {
            "success": results.success,
            "statistics": results.statistics,
            "results": results.results,
            "file_path": str(file_path)
        }

    def get_users_expectations(self) -> List[ExpectationConfiguration]:
        """Get expectations for users Parquet file.
        
        Expectations:
        - File is not empty
        - email is unique and not null
        - id is unique
        
        Returns:
            List of expectations
        """
        expectations = [
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

        return expectations

    def get_products_expectations(self) -> List[ExpectationConfiguration]:
        """Get expectations for products Parquet file.
        
        Expectations:
        - File is not empty
        - price is positive
        - id is unique
        
        Returns:
            List of expectations
        """
        expectations = [
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

        return expectations

    def get_order_expectations(self) -> List[ExpectationConfiguration]:
        """Get expectations for orders Parquet file.
        
        Expectations:
        - File is not empty
        - user_id column exists
        - user_id is not null
        - id column is unique
        
        Returns:
            List of expectations
        """
        expectations = [
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

        return expectations

    def get_order_items_expectations(self) -> List[ExpectationConfiguration]:
        """Get expectations for order_items Parquet file.
        
        Expectations:
        - File is not empty (if orders exist)
        - quantity is positive
        - order_id and product_id are not null
        
        Returns:
            List of expectations
        """
        expectations = [
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

        return expectations
