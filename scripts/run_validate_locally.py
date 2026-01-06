"""Execute validation locally."""
import argparse
from datetime import datetime

from scripts.core.data_quality_validator import DataQualityValidator
from scripts.utils.file import check_file_exists, get_data_lake_path


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
    orders_path = get_data_lake_path("orders", execution_date)
    if check_file_exists(orders_path):
        try:
            result = validator.validate_parquet_file("orders", orders_path)
            results["orders"] = result

            if result["success"]:
                print("Orders validation PASSED")
                print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
            else:
                print("Orders validation FAILED")
                all_passed = False
                _print_failures(result)
        except Exception as e:
            print(f"Orders validation ERROR: {e}")
            all_passed = False
    else:
        print(f"No data found for orders for {execution_date}. Skipping validation.")

    # Validate order items
    print("\n2. Validating order items...")
    order_items_path = get_data_lake_path("order_items", execution_date)
    if check_file_exists(order_items_path):
        try:
            result = validator.validate_parquet_file("order_items", order_items_path)
            results["order_items"] = result

            if result["success"]:
                print("Order items validation PASSED")
                print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
            else:
                print("Order items validation FAILED")
                all_passed = False
                _print_failures(result)
        except Exception as e:
            print(f"Order items validation ERROR: {e}")
            all_passed = False
    else:
        print(f"No data found for order items for {execution_date}. Skipping validation.")

    # Validate users
    print("\n2. Validating users...")
    users_path = get_data_lake_path("users", execution_date)
    if check_file_exists(users_path):
        try:
            result = validator.validate_parquet_file("users", users_path)
            results["users"] = result

            if result["success"]:
                print("Users validation PASSED")
                print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
            else:
                print("Users validation FAILED")
                all_passed = False
                _print_failures(result)
        except Exception as e:
            print(f"Users validation ERROR: {e}")
            all_passed = False
    else:
        print(f"No data found for users for {execution_date}. Skipping validation.")

    # Validate products
    print("\n3. Validating products...")
    products_path = get_data_lake_path("products", execution_date)
    if check_file_exists(products_path):
        try:
            result = validator.validate_parquet_file("products", products_path)
            results["products"] = result

            if result["success"]:
                print("Products validation PASSED")
                print(f"Validated {result['statistics']['evaluated_expectations']} expectations")
            else:
                print("Products validation FAILED")
                all_passed = False
                _print_failures(result)
        except Exception as e:
            print(f"Products validation ERROR: {e}")
            all_passed = False
    else:
        print(f"No data found for products for {execution_date}. Skipping validation.")

    # Print summary
    if all_passed:
        print("\nALL VALIDATIONS PASSED")
        print(f"Data quality check completed successfully for {execution_date}")
        return True
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
    print("\nValidation completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
