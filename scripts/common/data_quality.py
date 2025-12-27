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
        context_name: str,
        file_path: Path,
        expectations: List[ExpectationConfiguration],
    ) -> dict:
        """Validate a Parquet file against expectations using GX.
        
        Args:
            file_path: Path to Parquet file
            expectations: List of expectation configurations
            
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
        data_source = self.context.data_sources.add_pandas(f"{context_name}_data_source")
        
        # Add dataframe asset
        data_asset = data_source.add_dataframe_asset(name=f"{context_name}_data_asset")
        
        # Build batch definition
        batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{context_name}_batch")
        
        # Link the batch to the dataframe
        batch_parameters = {"dataframe": df}
        validator = batch_definition.get_batch(batch_parameters)

        suite = gx.ExpectationSuite(f"{context_name}_suite")
        suite.add_expectation_configurations(expectations)

        # Run validation
        results = validator.validate(suite)

        return {
            "success": results.success,
            "statistics": results.statistics,
            "results": results.results,
            "file_path": str(file_path)
        }

    def validate_orders(self, context_name: str, file_path: Path) -> List[ExpectationConfiguration]:
        """Validate orders Parquet file.
        
        Expectations:
        - File is not empty
        - user_id column exists
        - user_id is not null
        - id column is unique
        
        Args:
            file_path: Path to orders Parquet file
            
        Returns:
            Validation results
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

        return self.validate_parquet_file(context_name, file_path, expectations)

    def validate_marketing(self, context_name: str, file_path: Path) -> List[ExpectationConfiguration]:
        """Validate marketing Parquet file.
        
        Expectations:
        - File is not empty
        - spend column exists
        - spend is greater than 0
        - platform is in expected set
        
        Args:
            file_path: Path to marketing Parquet file
            
        Returns:
            Validation results
        """
        expectations = [
            ExpectationConfiguration(
                type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1}
            ),
            ExpectationConfiguration(
                type="expect_column_to_exist",
                kwargs={"column": "spend"}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_between",
                kwargs={"column": "spend", "min_value": 0, "strict_min": True}
            ),
            ExpectationConfiguration(
                type="expect_column_values_to_be_in_set",
                kwargs={"column": "platform", "value_set": ["Facebook", "Google", "LinkedIn"]}
            )
        ]

        return self.validate_parquet_file(context_name, file_path, expectations)

    def validate_order_items(self, context_name: str, file_path: Path) -> List[ExpectationConfiguration]:
        """Validate order_items Parquet file.
        
        Expectations:
        - File is not empty (if orders exist)
        - quantity is positive
        - order_id and product_id are not null
        
        Args:
            file_path: Path to order_items Parquet file
            
        Returns:
            Validation results
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

        return self.validate_parquet_file(context_name, file_path, expectations)

    def validate_users(self, context_name: str, file_path: Path) -> List[ExpectationConfiguration]:
        """Validate users Parquet file.
        
        Expectations:
        - File is not empty
        - email is unique and not null
        - id is unique
        
        Args:
            file_path: Path to users Parquet file
            
        Returns:
            Validation results
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

        return self.validate_parquet_file(context_name, file_path, expectations)

    def validate_products(self, context_name: str, file_path: Path) -> List[ExpectationConfiguration]:
        """Validate products Parquet file.
        
        Expectations:
        - File is not empty
        - price is positive
        - id is unique
        
        Args:
            file_path: Path to products Parquet file
            
        Returns:
            Validation results
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

        return self.validate_parquet_file(context_name, file_path, expectations)
