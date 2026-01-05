import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from scripts.core.data_quality_validator import DataQualityValidator


@pytest.fixture
def temp_parquet_file(tmp_path):
    """Create a temporary parquet file for testing."""

    def _create_file(data):
        file_path = tmp_path / "test.parquet"
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False)
        return file_path

    return _create_file


def test_validator_initialization():
    """Test that validator initializes correctly."""
    validator = DataQualityValidator()
    assert validator.context is not None


def test_validate_orders_success(temp_parquet_file):
    """Test successful orders validation."""
    data = {
        "id": [1, 2, 3],
        "user_id": [10, 20, 30],
        "status": ["completed", "pending", "shipped"],
        "created_at": pd.date_range("2024-01-01", periods=3),
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("orders", file_path)

    assert result["success"] is True
    assert result["file_path"] == str(file_path)


def test_validate_orders_null_user_id(temp_parquet_file):
    """Test orders validation fails with null user_id."""
    data = {
        "id": [1, 2, 3],
        "user_id": [10, None, 30],  # Null value
        "status": ["completed", "pending", "shipped"],
        "created_at": pd.date_range("2024-01-01", periods=3),
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("orders", file_path)

    assert result["success"] is False


def test_validate_orders_empty_file(temp_parquet_file):
    """Test orders validation fails with empty file."""
    data = {"id": [], "user_id": [], "status": [], "created_at": []}
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("orders", file_path)

    assert result["success"] is False


def test_validate_orders_duplicate_id(temp_parquet_file):
    """Test orders validation fails with duplicate id."""
    data = {
        "id": [1, 1, 3],  # Duplicate id
        "user_id": [10, 20, 30],
        "status": ["completed", "pending", "shipped"],
        "created_at": pd.date_range("2024-01-01", periods=3),
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("orders", file_path)

    assert result["success"] is False


def test_validate_order_items_success(temp_parquet_file):
    """Test successful order items validation."""
    data = {
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)

    assert result["success"] is True


def test_validate_order_items_zero_quantity(temp_parquet_file):
    """Test order items validation fails with zero quantity."""
    data = {
        "id": [1, 2],
        "order_id": [100, 101],
        "product_id": [50, 51],
        "quantity": [2, 0],  # Zero quantity
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)

    assert result["success"] is False


def test_validate_order_items_null_order_id(temp_parquet_file):
    """Test order items validation fails with null order_id."""
    data = {
        "id": [1, 2],
        "order_id": [100, None],  # Null order_id
        "product_id": [50, 51],
        "quantity": [2, 1],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)

    assert result["success"] is False


def test_validate_order_items_null_product_id(temp_parquet_file):
    """Test order items validation fails with null product_id."""
    data = {
        "id": [1, 2],
        "order_id": [100, 101],
        "product_id": [50, None],  # Null product_id
        "quantity": [2, 1],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)

    assert result["success"] is False


def test_validate_users_success(temp_parquet_file):
    """Test successful users validation."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("users", file_path)

    assert result["success"] is True


def test_validate_users_duplicate_email(temp_parquet_file):
    """Test users validation fails with duplicate email."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": [
            "alice@example.com",
            "alice@example.com",  # Duplicate
            "charlie@example.com",
        ],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("users", file_path)

    assert result["success"] is False


def test_validate_users_null_email(temp_parquet_file):
    """Test users validation fails with null email."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@example.com", None, "charlie@example.com"],  # Null email
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("users", file_path)

    assert result["success"] is False


def test_validate_products_success(temp_parquet_file):
    """Test successful products validation."""
    data = {
        "id": [1, 2, 3],
        "name": ["Product A", "Product B", "Product C"],
        "category": ["Electronics", "Clothing", "Books"],
        "price": [99.99, 49.99, 19.99],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("products", file_path)

    assert result["success"] is True


def test_validate_products_zero_price(temp_parquet_file):
    """Test products validation fails with zero price."""
    data = {
        "id": [1, 2],
        "name": ["Product A", "Product B"],
        "category": ["Electronics", "Clothing"],
        "price": [99.99, 0.0],  # Zero price
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("products", file_path)

    assert result["success"] is False


def test_validate_products_duplicate_id(temp_parquet_file):
    """Test products validation fails with duplicate id."""
    data = {
        "id": [1, 1, 3],  # Duplicate id
        "name": ["Product A", "Product B", "Product C"],
        "category": ["Electronics", "Clothing", "Books"],
        "price": [99.99, 49.99, 19.99],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_parquet_file("products", file_path)

    assert result["success"] is False


def test_validate_file_not_found():
    """Test validation fails when file doesn't exist."""
    validator = DataQualityValidator()

    with pytest.raises(FileNotFoundError):
        validator.validate_parquet_file("orders", Path("/nonexistent/file.parquet"))


def test_unknown_table(temp_parquet_file):
    """Test validation fails when unknown table is provided."""
    data = {
        "id": [1, 2],
        "title": ["Title A", "Title B"],
        "description": ["Description A", "Description B"]
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    with pytest.raises(ValueError):
        validator.validate_parquet_file("unknown_table", file_path)

def test_rename_invalid_file_on_validation_failure(temp_parquet_file):
    """Test that invalid file is renamed when validation fails."""
    # Create invalid data (null order_id)
    data = {
        "id": [1, 2],
        "order_id": [100, None],  # Null order_id - will fail validation
        "product_id": [50, 51],
        "quantity": [2, 1],
    }
    file_path = temp_parquet_file(data)
    original_path = Path(file_path)
    
    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)
    
    # Validation should fail
    assert result["success"] is False
    
    # File should be renamed
    assert "renamed_to" in result
    renamed_path = Path(result["renamed_to"])
    
    # Renamed file should exist
    assert renamed_path.exists()
    
    # Original file should NOT exist
    assert not original_path.exists()


def test_rename_invalid_file_format(temp_parquet_file):
    """Test that renamed file follows correct format: {original}.invalid.{YYYYMMDD_HHmmss}."""
    # Create invalid data
    data = {
        "id": [1, 2],
        "order_id": [100, None],
        "product_id": [50, 51],
        "quantity": [2, 1],
    }
    file_path = temp_parquet_file(data)
    
    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)
    
    assert result["success"] is False
    assert "renamed_to" in result
    
    renamed_path = Path(result["renamed_to"])
    
    # Check filename format: test.parquet.invalid.YYYYMMDD_HHMMSS
    pattern = r"^test\.parquet\.invalid\.\d{8}_\d{6}$"
    assert re.match(pattern, renamed_path.name), f"Filename {renamed_path.name} doesn't match expected pattern"


def test_rename_on_failure_false(temp_parquet_file):
    """Test that file is NOT renamed when rename_on_failure=False."""
    # Create invalid data
    data = {
        "id": [1, 2],
        "order_id": [100, None],
        "product_id": [50, 51],
        "quantity": [2, 1],
    }
    file_path = temp_parquet_file(data)
    original_path = Path(file_path)
    
    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path, rename_on_failure=False)
    
    # Validation should fail
    assert result["success"] is False
    
    # File should NOT be renamed
    assert "renamed_to" not in result
    
    # Original file should still exist
    assert original_path.exists()


def test_valid_file_not_renamed(temp_parquet_file):
    """Test that valid file is not renamed."""
    # Create valid data
    data = {
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    }
    file_path = temp_parquet_file(data)
    original_path = Path(file_path)
    
    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)
    
    # Validation should pass
    assert result["success"] is True
    
    # File should NOT be renamed
    assert "renamed_to" not in result
    
    # Original file should still exist
    assert original_path.exists()


def test_rename_invalid_file_preserves_parent_directory(temp_parquet_file):
    """Test that renamed file stays in the same directory."""
    # Create invalid data
    data = {
        "id": [1, 2],
        "order_id": [100, None],
        "product_id": [50, 51],
        "quantity": [2, 1],
    }
    file_path = temp_parquet_file(data)
    original_path = Path(file_path)
    original_parent = original_path.parent
    
    validator = DataQualityValidator()
    result = validator.validate_parquet_file("order_items", file_path)
    
    assert result["success"] is False
    assert "renamed_to" in result
    
    renamed_path = Path(result["renamed_to"])
    
    # Renamed file should be in the same directory
    assert renamed_path.parent == original_parent
