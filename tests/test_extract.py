import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from scripts.extractors.db_extractor import (
    extract_child_table_by_parent_table,
    extract_table_by_date,
)


# Database Extractor Tests
@pytest.fixture
def mock_db_connection():
    """Mock database connection for testing."""
    with patch("scripts.extractors.db_extractor.get_source_db_connection") as mock_conn:
        mock_connection = MagicMock()
        mock_conn.return_value = mock_connection
        yield mock_connection


@pytest.fixture
def mock_read_sql():
    """Mock pandas read_sql_query."""
    with patch("scripts.extractors.db_extractor.pd.read_sql_query") as mock_read:
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
    """Test extracting table filtered by date using updated_at."""
    monkeypatch.chdir(tmp_path)
    mock_read_sql.return_value = sample_orders_df
    output_path = f"{tmp_path}/data/orders/2025-12-01.parquet"
    
    extract_table_by_date("orders", "2025-12-01")
        
    # Verify SQL query was called with correct parameters
    mock_read_sql.assert_called_once()
    call_args = mock_read_sql.call_args
    assert "orders" in call_args[0][0]
    assert "updated_at::DATE" in call_args[0][0]
    assert call_args[1]["params"] == ("2025-12-01",)
    
    # Verify file was created
    assert os.path.exists(output_path)
    
    # Verify file is valid Parquet
    df = pd.read_parquet(output_path)
    assert isinstance(df, pd.DataFrame)
    
    # Verify data in parquet file
    pd.testing.assert_frame_equal(df, sample_orders_df)
    
    # Verify connection cleanup
    mock_db_connection.close.assert_called_once()


def test_extract_table_by_date_custom_column(mock_read_sql, sample_orders_df, tmp_path, monkeypatch):
    """Test extracting table with custom date column."""
    monkeypatch.chdir(tmp_path)
    mock_read_sql.return_value = sample_orders_df
    
    extract_table_by_date("orders", "2025-12-01", date_column="created_at")
        
    # Verify SQL uses custom column
    call_args = mock_read_sql.call_args
    assert "created_at::DATE" in call_args[0][0]
    

def test_extract_child_table_by_parent_table(mock_db_connection, mock_read_sql, tmp_path, monkeypatch):
    """Test extracting child table based on parent table date."""
    order_items_df = pd.DataFrame({
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    })
    mock_read_sql.return_value = order_items_df
    monkeypatch.chdir(tmp_path)
    output_path = f"{tmp_path}/data/order_items/2025-12-01.parquet"

    extract_child_table_by_parent_table(
        "orders", "order_items", "order_id", "2025-12-01"
    )
        
    # Verify SQL includes JOIN with parent table
    call_args = mock_read_sql.call_args
    assert "order_items" in call_args[0][0]
    assert "JOIN orders" in call_args[0][0]
    assert "order_id" in call_args[0][0]
    assert "updated_at::DATE" in call_args[0][0]
    
    # Verify file was created
    assert os.path.exists(output_path)
    
    # Verify file is valid Parquet
    df = pd.read_parquet(output_path)
    assert isinstance(df, pd.DataFrame)
    
    # Verify data in parquet file
    pd.testing.assert_frame_equal(df, order_items_df)
    
    # Verify connection cleanup
    mock_db_connection.close.assert_called_once()


def test_extract_table_by_date_empty_result(mock_read_sql, tmp_path, monkeypatch):
    """Test extracting table when no data for date."""
    empty_df = pd.DataFrame(columns=["id", "user_id", "status", "created_at", "updated_at"])
    mock_read_sql.return_value = empty_df
    monkeypatch.chdir(tmp_path)
    output_path = f"{tmp_path}/data/orders/1970-01-01.parquet"
        
    extract_table_by_date("orders", "1970-01-01")
        
    # Should still create file even if empty
    assert os.path.exists(output_path)

    # Verify that the file is empty
    df = pd.read_parquet(output_path)
    assert len(df) == 0


def test_extract_table_connection_cleanup_on_error(mock_db_connection, mock_read_sql):
    """Test that connection is closed even when error occurs."""
    mock_read_sql.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        extract_table_by_date("orders", "2025-12-01")
    
    # Connection should still be closed
    mock_db_connection.close.assert_called_once()
