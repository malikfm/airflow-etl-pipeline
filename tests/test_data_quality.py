from pathlib import Path

import pandas as pd
import pytest

from scripts.common.data_quality import DataQualityValidator


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
    result = validator.validate_orders(file_path)

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
    result = validator.validate_orders(file_path)

    assert result["success"] is False


def test_validate_orders_empty_file(temp_parquet_file):
    """Test orders validation fails with empty file."""
    data = {"id": [], "user_id": [], "status": [], "created_at": []}
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_orders(file_path)

    assert result["success"] is False


def test_validate_marketing_success(temp_parquet_file):
    """Test successful marketing validation."""
    data = {
        "campaign_id": ["CMP-001", "CMP-002"],
        "date": pd.date_range("2024-01-01", periods=2),
        "platform": ["Facebook", "Google"],
        "spend": [1000.50, 2000.75],
        "clicks": [500, 800],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_marketing(file_path)

    assert result["success"] is True


def test_validate_marketing_negative_spend(temp_parquet_file):
    """Test marketing validation fails with negative spend."""
    data = {
        "campaign_id": ["CMP-001", "CMP-002"],
        "date": pd.date_range("2024-01-01", periods=2),
        "platform": ["Facebook", "Google"],
        "spend": [1000.50, -100.0],  # Negative spend
        "clicks": [500, 800],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_marketing(file_path)

    assert result["success"] is False


def test_validate_marketing_zero_spend(temp_parquet_file):
    """Test marketing validation fails with zero spend."""
    data = {
        "campaign_id": ["CMP-001"],
        "date": pd.date_range("2024-01-01", periods=1),
        "platform": ["Facebook"],
        "spend": [0.0],  # Zero spend (should be > 0)
        "clicks": [500],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_marketing(file_path)

    assert result["success"] is False


def test_validate_marketing_invalid_platform(temp_parquet_file):
    """Test marketing validation fails with invalid platform."""
    data = {
        "campaign_id": ["CMP-001"],
        "date": pd.date_range("2024-01-01", periods=1),
        "platform": ["Twitter"],  # Invalid platform
        "spend": [1000.0],
        "clicks": [500],
    }
    file_path = temp_parquet_file(data)

    validator = DataQualityValidator()
    result = validator.validate_marketing(file_path)

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
    result = validator.validate_order_items(file_path)

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
    result = validator.validate_order_items(file_path)

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
    result = validator.validate_users(file_path)

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
    result = validator.validate_users(file_path)

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
    result = validator.validate_products(file_path)

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
    result = validator.validate_products(file_path)

    assert result["success"] is False


def test_validate_file_not_found():
    """Test validation fails when file doesn't exist."""
    validator = DataQualityValidator()

    with pytest.raises(FileNotFoundError):
        validator.validate_orders(Path("/nonexistent/file.parquet"))
