from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.loaders.staging_loader import truncate_and_load


@pytest.fixture
def temp_parquet_file(tmp_path):
    """Create a temporary parquet file for testing."""

    def _create_file(data, filename="test.parquet"):
        file_path = tmp_path / filename
        df = pd.DataFrame(data)
        df.to_parquet(file_path, index=False)
        return file_path

    return _create_file


@pytest.fixture
def mock_db_connection():
    """Mock database connection."""
    with patch("scripts.loaders.staging_loader.get_dw_db_connection") as mock_conn:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.return_value = mock_connection
        yield mock_connection, mock_cursor


@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock SQLAlchemy engine."""
    with patch("scripts.loaders.staging_loader.create_engine") as mock_engine:
        yield mock_engine


def test_truncate_and_load_file_not_found():
    """Test that FileNotFoundError is raised when file doesn't exist."""
    non_existent_path = Path("/nonexistent/file.parquet")

    with pytest.raises(FileNotFoundError):
        truncate_and_load("orders", non_existent_path)


def test_truncate_and_load_empty_file(temp_parquet_file, mock_db_connection):
    """Test handling of empty parquet file."""
    # Create empty parquet file
    empty_data = {"id": [], "name": []}
    file_path = temp_parquet_file(empty_data)

    mock_connection, mock_cursor = mock_db_connection

    result = truncate_and_load("test_table", file_path)

    # Should return 0 for empty file
    assert result == 0
    # Connection should not be opened for empty files (early return)
    mock_connection.close.assert_not_called()


def test_truncate_and_load_success(
    temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine
):
    """Test successful truncate and load operation."""
    # Create test data
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300],
    }
    file_path = temp_parquet_file(data)

    mock_connection, mock_cursor = mock_db_connection
    mock_engine = MagicMock()
    mock_sqlalchemy_engine.return_value = mock_engine

    # Mock DataFrame.to_sql to avoid actual database write
    with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
        result = truncate_and_load("test_table", file_path)

        # Verify schema creation was called
        schema_calls = [
            call for call in mock_cursor.execute.call_args_list
            if "CREATE SCHEMA" in str(call)
        ]
        assert len(schema_calls) > 0

        # Verify table creation was called
        table_calls = [
            call for call in mock_cursor.execute.call_args_list
            if "CREATE TABLE" in str(call)
        ]
        assert len(table_calls) > 0

        # Verify to_sql was called with correct parameters
        mock_to_sql.assert_called_once()
        call_kwargs = mock_to_sql.call_args[1]
        assert call_kwargs["schema"] == "staging"
        assert call_kwargs["if_exists"] == "delete_rows"  # truncate
        assert call_kwargs["index"] is False
        assert call_kwargs["method"] == "multi"

        # Verify row count
        assert result == 3

        # Verify connection cleanup
        mock_connection.close.assert_called_once()


def test_truncate_and_load_orders(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test loading orders data."""
    data = {
        "id": [1, 2],
        "user_id": [10, 20],
        "status": ["completed", "pending"],
        "created_at": pd.date_range("2025-12-01", periods=2),
        "updated_at": pd.date_range("2025-12-01", periods=2),
    }
    file_path = temp_parquet_file(data, "orders.parquet")

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("orders", file_path)

        assert result == 2
        # Verify table name in creation
        table_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "orders" in str(call)
        ]
        assert len(table_calls) > 0


def test_truncate_and_load_order_items(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test loading order items data."""
    data = {
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    }
    file_path = temp_parquet_file(data, "order_items.parquet")

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("order_items", file_path)

        assert result == 3
        table_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "order_items" in str(call)
        ]
        assert len(table_calls) > 0


def test_truncate_and_load_users(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test loading users data."""
    data = {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
        "created_at": pd.date_range("2025-12-01", periods=3),
        "updated_at": pd.date_range("2025-12-01", periods=3),
        "deleted_at": [None, None, None],
    }
    file_path = temp_parquet_file(data, "users.parquet")

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("users", file_path)

        assert result == 3
        table_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "users" in str(call)
        ]
        assert len(table_calls) > 0


def test_truncate_and_load_products(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test loading products data."""
    data = {
        "id": [1, 2, 3],
        "name": ["Product A", "Product B", "Product C"],
        "category": ["Electronics", "Clothing", "Books"],
        "price": [99.99, 49.99, 19.99],
        "created_at": pd.date_range("2025-12-01", periods=3),
        "updated_at": pd.date_range("2025-12-01", periods=3),
        "deleted_at": [None, None, None],
    }
    file_path = temp_parquet_file(data, "products.parquet")

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("products", file_path)

        assert result == 3
        table_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "products" in str(call)
        ]
        assert len(table_calls) > 0


def test_truncate_and_load_with_different_dtypes(
    temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine
):
    """Test loading data with various data types."""
    data = {
        "int_col": [1, 2, 3],
        "float_col": [1.1, 2.2, 3.3],
        "str_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "datetime_col": pd.date_range("2025-12-01", periods=3),
    }
    file_path = temp_parquet_file(data)

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("test_types", file_path)

        assert result == 3

        # Verify table creation includes all column types
        create_table_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "CREATE TABLE" in str(call)
        ]
        assert len(create_table_calls) > 0

        # Verify type mapping
        create_sql = str(create_table_calls[0])
        assert "BIGINT" in create_sql or "int_col" in create_sql
        assert "DOUBLE PRECISION" in create_sql or "float_col" in create_sql
        assert "TEXT" in create_sql or "str_col" in create_sql
        assert "TIMESTAMP" in create_sql or "datetime_col" in create_sql


def test_truncate_and_load_idempotency(
    temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine
):
    """Test that loading same data twice produces same result (idempotency)."""
    data = {"id": [1, 2, 3], "value": [100, 200, 300]}
    file_path = temp_parquet_file(data)

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        # Load first time
        result1 = truncate_and_load("test_table", file_path)

        # Reset mocks
        mock_cursor.reset_mock()
        mock_connection.reset_mock()

        # Load second time (should truncate and reload)
        result2 = truncate_and_load("test_table", file_path)

        # Both should return same row count
        assert result1 == result2 == 3

        # Verify schema and table creation was called second time too
        assert mock_cursor.execute.call_count > 0
