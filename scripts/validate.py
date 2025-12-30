"""Execute validation locally."""
import argparse
from datetime import datetime

from scripts.validations.data_quality import DataQualityValidator
from scripts.common.file_utils import get_data_lake_path


def validate_extraction(execution_date: str) -> bool:
    """Validate all extracted files for a given execution date.
    
    This implements the Circuit Breaker pattern, if any validation
    fails, the pipeline should stop.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        True if all validations pass, False otherwise
        
    Raises:
        SystemExit: If validation fails (for use in pipelines)
    """
    print(f"\nValidating extracted data for {execution_date}")

    validator = DataQualityValidator()
    all_passed = True
    results = {}

    # Validate orders
    print("\n1. Validating orders...")
    try:
        orders_path = get_data_lake_path("orders", execution_date)
        result = validator.validate_parquet_file("orders", orders_path)
        results["orders"] = result

        if result["success"]:
            print(f"Orders validation PASSED")
            print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
        else:
            print(f"Orders validation FAILED")
            all_passed = False
            _print_failures(result)
    except Exception as e:
        print(f"Orders validation ERROR: {e}")
        all_passed = False

    # Validate order items
    print("\n2. Validating order items...")
    try:
        order_items_path = get_data_lake_path("order_items", execution_date)
        result = validator.validate_parquet_file("order_items", order_items_path)
        results["order_items"] = result

        if result["success"]:
            print(f"Order items validation PASSED")
            print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
        else:
            print(f"Order items validation FAILED")
            all_passed = False
            _print_failures(result)
    except Exception as e:
        print(f"Order items validation ERROR: {e}")
        all_passed = False

    # Validate users
    print("\n3. Validating users...")
    try:
        users_path = get_data_lake_path("users", execution_date)
        result = validator.validate_parquet_file("users", users_path)
        results["users"] = result

        if result["success"]:
            print(f"Users validation PASSED")
            print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
        else:
            print(f"Users validation FAILED")
            all_passed = False
            _print_failures(result)
    except Exception as e:
        print(f"Users validation ERROR: {e}")
        all_passed = False

    # Validate products
    print("\n4. Validating products...")
    try:
        products_path = get_data_lake_path("products", execution_date)
        result = validator.validate_parquet_file("products", products_path)
        results["products"] = result

        if result["success"]:
            print(f"Products validation PASSED")
            print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
        else:
            print(f"Products validation FAILED")
            all_passed = False
            _print_failures(result)
    except Exception as e:
        print(f"Products validation ERROR: {e}")
        all_passed = False

    # Print summary
    if all_passed:
        print("\nALL VALIDATIONS PASSED")
        print(f"Data quality check completed successfully for {execution_date}")
        return True
    else:
        print("\nVALIDATION FAILED")
        print("Pipeline will stop. Please fix data quality issues.")
        return False


def _print_failures(result: dict) -> None:
    """Print details of failed expectations."""
    for expectation_result in result["results"]:
        if not expectation_result.success:
            print(f"Failed: {expectation_result.expectation_config.expectation_type}")
            if hasattr(expectation_result, "result"):
                print(f"Details: {expectation_result.result}")


def main():
    parser = argparse.ArgumentParser(description="Extract data sources for a given execution date.")
    parser.add_argument("--execution-date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    execution_date = args.execution_date
    
    # Validate date format
    try:
        datetime.strptime(execution_date, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{execution_date}'. Use YYYY-MM-DD.")
        return 1

    # Run validation
    success = validate_extraction(execution_date)

    # Exit with appropriate code
    if not success:
        print("\nExiting with error code 1")
        return 1
    else:
        print("\nValidation completed successfully")


if __name__ == "__main__":
    raise SystemExit(main())
