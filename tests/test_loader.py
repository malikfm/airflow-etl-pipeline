from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.core.loader import truncate_and_load


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
    with patch("scripts.core.loader.get_dwh_db_connection") as mock_conn:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.return_value = mock_connection
        yield mock_connection, mock_cursor


@pytest.fixture
def mock_sqlalchemy_engine():
    """Mock SQLAlchemy engine."""
    with patch("scripts.core.loader.create_engine") as mock_engine:
        yield mock_engine


def test_truncate_and_load_file_not_found():
    """Test that 0 is returned when file doesn't exist (with warning)."""
    non_existent_path = Path("/nonexistent/file.parquet")

    # Should return 0 and print warning, not raise exception
    result = truncate_and_load("orders", non_existent_path, "20260101")
    assert result == 0


def test_truncate_and_load_empty_file(temp_parquet_file):
    """Test handling of empty parquet file."""
    # Create empty parquet file
    empty_data = {"id": [], "name": []}
    file_path = temp_parquet_file(empty_data)

    # Should return 0 for empty file without connecting to DB
    result = truncate_and_load("test_table", file_path, "20260101")
    assert result == 0


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
    row_count = len(data["id"])

    mock_connection, mock_cursor = mock_db_connection
    mock_engine = MagicMock()
    mock_sqlalchemy_engine.return_value = mock_engine

    batch_id = "20260101"

    # Mock DataFrame.to_sql to avoid actual database write
    with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
        result = truncate_and_load("test_table", file_path, batch_id)

        # Verify DELETE was called with batch_id
        delete_calls = [
            call for call in mock_cursor.execute.call_args_list
            if "DELETE" in str(call)
        ]
        assert len(delete_calls) == 1
        assert batch_id in str(delete_calls[0])

        # Verify to_sql was called with correct parameters
        mock_to_sql.assert_called_once()
        call_kwargs = mock_to_sql.call_args[1]
        assert call_kwargs["schema"] == "raw_ingest"
        assert call_kwargs["if_exists"] == "append"
        assert call_kwargs["index"] is False
        assert call_kwargs["method"] == "multi"

        # Verify row count (3 original rows)
        assert result == row_count

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
    row_count = len(data["id"])

    _, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("orders", file_path, "20260101")

        assert result == row_count

        # Verify DELETE was called for raw_ingest.orders
        delete_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "raw_ingest.orders" in str(call) and "DELETE" in str(call)
        ]
        assert len(delete_calls) == 1


def test_truncate_and_load_order_items(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test loading order items data."""
    data = {
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    }
    file_path = temp_parquet_file(data, "order_items.parquet")
    row_count = len(data["id"])

    _, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("order_items", file_path, "20260101")

        assert result == row_count

        # Verify DELETE was called for raw_ingest.order_items
        delete_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "raw_ingest.order_items" in str(call) and "DELETE" in str(call)
        ]
        assert len(delete_calls) == 1


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
    row_count = len(data["id"])

    _, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("users", file_path, "20260101")

        assert result == row_count

        # Verify DELETE was called for raw_ingest.users
        delete_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "raw_ingest.users" in str(call) and "DELETE" in str(call)
        ]
        assert len(delete_calls) == 1


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
    row_count = len(data["id"])

    _, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("products", file_path, "20260101")

        assert result == row_count

        # Verify DELETE was called for raw_ingest.products
        delete_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "raw_ingest.products" in str(call) and "DELETE" in str(call)
        ]
        assert len(delete_calls) == 1


@pytest.mark.usefixtures("mock_db_connection")
def test_truncate_and_load_with_different_dtypes(temp_parquet_file, mock_sqlalchemy_engine):
    """Test loading data with various data types."""
    data = {
        "int_col": [1, 2, 3],
        "float_col": [1.1, 2.2, 3.3],
        "str_col": ["a", "b", "c"],
        "bool_col": [True, False, True],
        "datetime_col": pd.date_range("2025-12-01", periods=3),
    }
    file_path = temp_parquet_file(data)
    row_count = len(data["int_col"])

    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        result = truncate_and_load("test_types", file_path, "20260101")

        assert result == row_count


def test_truncate_and_load_idempotency(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test that loading same data twice produces same result (idempotency)."""
    data = {"id": [1, 2, 3], "value": [100, 200, 300]}
    file_path = temp_parquet_file(data)
    row_count = len(data["id"])

    mock_connection, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    batch_id = "20260101"

    with patch.object(pd.DataFrame, "to_sql"):
        # Load first time
        result1 = truncate_and_load("test_table", file_path, batch_id)

        # Reset mocks
        mock_cursor.reset_mock()
        mock_connection.reset_mock()

        # Load second time (should delete by batch_id and reload)
        result2 = truncate_and_load("test_table", file_path, batch_id)

        # Both should return same row count
        assert result1 == result2 == row_count

        # Verify DELETE was called second time too
        delete_calls = [
            str(call) for call in mock_cursor.execute.call_args_list
            if "DELETE" in str(call)
        ]
        assert len(delete_calls) == 1


def test_truncate_and_load_different_batch_ids(temp_parquet_file, mock_db_connection, mock_sqlalchemy_engine):
    """Test loading with different batch_ids deletes only matching batch."""
    data = {"id": [1, 2], "value": [100, 200]}
    file_path = temp_parquet_file(data)

    _, mock_cursor = mock_db_connection
    mock_sqlalchemy_engine.return_value = MagicMock()

    with patch.object(pd.DataFrame, "to_sql"):
        # Load with first batch_id
        truncate_and_load("test_table", file_path, "20260101")

        # Verify DELETE uses correct batch_id
        delete_call = str(mock_cursor.execute.call_args_list[0])
        assert "20260101" in delete_call

        mock_cursor.reset_mock()

        # Load with second batch_id
        truncate_and_load("test_table", file_path, "20260102")

        # Verify DELETE uses new batch_id
        delete_call = str(mock_cursor.execute.call_args_list[0])
        assert "20260102" in delete_call
