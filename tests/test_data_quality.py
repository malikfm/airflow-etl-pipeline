from pathlib import Path

import pandas as pd
import pytest

from scripts.validations.data_quality import DataQualityValidator


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
