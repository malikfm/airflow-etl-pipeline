import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from scripts.core.extractor import (
    extract_child_table_by_parent_table,
    extract_table_by_date,
)


# Database Extractor Tests
@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing."""
    with patch("scripts.core.extractor.get_source_db_connection") as mock_conn:
        mock_connection = MagicMock()
        mock_conn.return_value = mock_connection
        yield mock_connection


@pytest.fixture
def mock_read_sql():
    """Mock pandas read_sql_query."""
    with patch("scripts.core.extractor.pd.read_sql_query") as mock_read:
        yield mock_read


@pytest.fixture
def sample_orders_df():
    """Sample orders DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "user_id": [10, 20, 30],
        "status": ["completed", "pending", "shipped"],
        "created_at": pd.date_range("2025-12-01", periods=3),
        "updated_at": pd.date_range("2025-12-01", periods=3),
    })


def test_extract_table_by_date(mock_db_connection, mock_read_sql, sample_orders_df, tmp_path, monkeypatch):
    """Test extracting table filtered by date using created_at OR updated_at."""
    monkeypatch.chdir(tmp_path)
    mock_read_sql.return_value = sample_orders_df
    output_path = f"{tmp_path}/data/orders/2025-12-01.parquet"

    extract_table_by_date("orders", "2025-12-01")

    # Verify SQL query was called with correct parameters
    mock_read_sql.assert_called_once()
    call_args = mock_read_sql.call_args
    assert "orders" in call_args[0][0]
    assert "created_at::DATE" in call_args[0][0]
    assert "updated_at::DATE" in call_args[0][0]
    assert "OR" in call_args[0][0]
    # Params passed twice for created_at and updated_at
    assert call_args[1]["params"] == ["2025-12-01", "2025-12-01"]

    # Verify file was created
    assert os.path.exists(output_path)

    # Verify file is valid Parquet
    df = pd.read_parquet(output_path)
    assert isinstance(df, pd.DataFrame)

    # Verify data in parquet file
    pd.testing.assert_frame_equal(df, sample_orders_df)

    # Verify connection cleanup
    mock_db_connection.close.assert_called_once()


def test_extract_child_table_by_parent_table(mock_db_connection, mock_read_sql, tmp_path, monkeypatch):
    """Test extracting child table based on parent table IDs from parquet file."""
    # First create parent parquet file
    parent_df = pd.DataFrame({
        "id": [100, 101, 102],
        "user_id": [1, 2, 3],
        "status": ["completed", "pending", "shipped"],
        "created_at": pd.date_range("2025-12-01", periods=3),
        "updated_at": pd.date_range("2025-12-01", periods=3),
    })

    order_items_df = pd.DataFrame({
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    })

    monkeypatch.chdir(tmp_path)

    # Create parent parquet file first
    parent_path = f"{tmp_path}/data/orders/2025-12-01.parquet"
    os.makedirs(os.path.dirname(parent_path), exist_ok=True)
    parent_df.to_parquet(parent_path, index=False)

    mock_read_sql.return_value = order_items_df
    output_path = f"{tmp_path}/data/order_items/2025-12-01.parquet"

    extract_child_table_by_parent_table(
        "orders", "order_items", "id", "order_id", "2025-12-01"
    )

    # Verify SQL includes IN clause with parent IDs
    call_args = mock_read_sql.call_args
    assert "order_items" in call_args[0][0]
    assert "WHERE order_id IN" in call_args[0][0]
    # Verify parent IDs are passed as tuple for IN clause
    assert call_args[1]["params"] == [(100, 101, 102)]

    # Verify file was created
    assert os.path.exists(output_path)

    # Verify file is valid Parquet
    df = pd.read_parquet(output_path)
    assert isinstance(df, pd.DataFrame)

    # Verify data in parquet file
    pd.testing.assert_frame_equal(df, order_items_df)

    # Verify connection cleanup
    mock_db_connection.close.assert_called_once()


def test_extract_table_by_date_empty_result(mock_read_sql, mock_db_connection, tmp_path, monkeypatch):
    """Test extracting table when no data for date returns None and creates no file."""
    empty_df = pd.DataFrame(columns=["id", "user_id", "status", "created_at", "updated_at"])
    mock_read_sql.return_value = empty_df
    monkeypatch.chdir(tmp_path)
    output_path = f"{tmp_path}/data/orders/1970-01-01.parquet"

    result = extract_table_by_date("orders", "1970-01-01")

    # Should return None when no data
    assert result is None

    # Should NOT create file when empty
    assert not os.path.exists(output_path)

    # Verify connection cleanup
    mock_db_connection.close.assert_called_once()


def test_extract_child_table_by_parent_table_empty_parent(tmp_path, monkeypatch):
    """Test extracting child table when parent has no data returns None."""
    # Create empty parent parquet file
    empty_parent_df = pd.DataFrame(columns=["id", "user_id", "status", "created_at", "updated_at"])

    monkeypatch.chdir(tmp_path)
    parent_path = f"{tmp_path}/data/orders/1970-01-01.parquet"
    os.makedirs(os.path.dirname(parent_path), exist_ok=True)
    empty_parent_df.to_parquet(parent_path, index=False)

    output_path = f"{tmp_path}/data/order_items/1970-01-01.parquet"

    result = extract_child_table_by_parent_table(
        "orders", "order_items", "id", "order_id", "1970-01-01"
    )

    # Should return None when parent is empty
    assert result is None

    # Should NOT create file
    assert not os.path.exists(output_path)


def test_extract_table_connection_cleanup_on_error(mock_db_connection, mock_read_sql):
    """Test that connection is closed even when error occurs."""
    mock_read_sql.side_effect = Exception("Database error")

    # Mock check_file_exists to return False so extraction is not skipped
    with patch("scripts.core.extractor.check_file_exists", return_value=False), pytest.raises(Exception):
        extract_table_by_date("orders", "2025-12-01")

    # Connection should still be closed
    mock_db_connection.close.assert_called_once()


def test_extract_table_by_date_file_exists_skip_extraction(
    mock_db_connection, mock_read_sql, sample_orders_df, tmp_path, monkeypatch
):
    """Test that extraction is skipped when file already exists."""
    empty_df = pd.DataFrame(columns=["id", "user_id", "status", "created_at", "updated_at"])
    mock_read_sql.return_value = empty_df
    monkeypatch.chdir(tmp_path)
    output_path = f"{tmp_path}/data/orders/2025-12-01.parquet"

    # Create the file first
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    sample_orders_df.to_parquet(output_path, index=False)

    # Now try to extract, should skip
    result_path = extract_table_by_date("orders", "2025-12-01")

    # Verify file path is returned
    assert str(result_path) == "data/orders/2025-12-01.parquet"

    # Verify SQL wasn't called (extraction skipped)
    mock_read_sql.assert_not_called()

    # Verify connection wasn't opened (extraction skipped)
    mock_db_connection.close.assert_not_called()

    # Verify file unchanged, not overwritten by empty_df
    df = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(df, sample_orders_df)


def test_extract_child_table_by_parent_table_file_exists_skip_extraction(
    mock_db_connection, mock_read_sql, tmp_path, monkeypatch
):
    """Test that child table extraction is skipped when file already exists."""
    # Create parent parquet file
    parent_df = pd.DataFrame({
        "id": [100, 101, 102],
        "user_id": [1, 2, 3],
        "status": ["completed", "pending", "shipped"],
        "created_at": pd.date_range("2025-12-01", periods=3),
        "updated_at": pd.date_range("2025-12-01", periods=3),
    })

    order_items_df = pd.DataFrame({
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    })
    empty_df = pd.DataFrame(columns=["id", "order_id", "product_id", "quantity"])

    mock_read_sql.return_value = empty_df
    monkeypatch.chdir(tmp_path)

    # Create parent parquet file first
    parent_path = f"{tmp_path}/data/orders/2025-12-01.parquet"
    os.makedirs(os.path.dirname(parent_path), exist_ok=True)
    parent_df.to_parquet(parent_path, index=False)

    # Create child file
    output_path = f"{tmp_path}/data/order_items/2025-12-01.parquet"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    order_items_df.to_parquet(output_path, index=False)

    # Now try to extract, should skip
    result_path = extract_child_table_by_parent_table(
        "orders", "order_items", "id", "order_id", "2025-12-01"
    )

    # Verify file path is returned
    assert str(result_path) == "data/order_items/2025-12-01.parquet"

    # Verify SQL wasn't called (extraction skipped)
    mock_read_sql.assert_not_called()

    # Verify connection wasn't opened (extraction skipped)
    mock_db_connection.close.assert_not_called()

    # Verify file unchanged, not overwritten by empty_df
    df = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(df, order_items_df)
