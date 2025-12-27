import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from scripts.extractors.api_extractor import json_to_dataframe, mock_marketing_api
from scripts.extractors.db_extractor import (
    extract_full_table,
    extract_order_items_by_orders,
    extract_orders,
    extract_table_by_date,
)


# API Extractor Tests
def test_mock_marketing_api_returns_data():
    """Test that mock API returns data for a given date."""
    result = mock_marketing_api("2024-01-01")
    
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(item, dict) for item in result)


def test_mock_marketing_api_has_required_fields():
    """Test that mock API returns all required fields."""
    result = mock_marketing_api("2024-01-01")
    
    required_fields = {"campaign_id", "date", "platform", "spend", "clicks"}
    
    for record in result:
        assert required_fields.issubset(record.keys())


def test_mock_marketing_api_date_matches():
    """Test that returned data has the correct date."""
    execution_date = "2024-01-15"
    result = mock_marketing_api(execution_date)
    
    for record in result:
        assert record["date"] == execution_date


def test_json_to_dataframe_success():
    """Test successful conversion of JSON to DataFrame."""
    json_data = [
        {
            "campaign_id": "CMP-001",
            "date": "2024-01-01",
            "platform": "Facebook",
            "spend": 1000.50,
            "clicks": 500,
        },
        {
            "campaign_id": "CMP-002",
            "date": "2024-01-01",
            "platform": "Google",
            "spend": 2000.75,
            "clicks": 800,
        },
    ]
    
    df = json_to_dataframe(json_data)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "campaign_id" in df.columns
    assert "date" in df.columns
    assert "platform" in df.columns


def test_json_to_dataframe_date_conversion():
    """Test that date column is converted to datetime."""
    json_data = [
        {
            "campaign_id": "CMP-001",
            "date": "2024-01-01",
            "platform": "Facebook",
            "spend": 1000.0,
        }
    ]
    
    df = json_to_dataframe(json_data)
    
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_json_to_dataframe_empty_raises_error():
    """Test that empty JSON data raises ValueError."""
    with pytest.raises(ValueError, match="JSON data is empty"):
        json_to_dataframe([])


def test_json_to_dataframe_preserves_data_types():
    """Test that data types are preserved correctly."""
    json_data = [
        {
            "campaign_id": "CMP-001",
            "date": "2024-01-01",
            "platform": "Facebook",
            "spend": 1000.50,
            "clicks": 500,
        }
    ]
    
    df = json_to_dataframe(json_data)
    
    assert df["campaign_id"].dtype == object  # string
    assert df["platform"].dtype == object  # string
    assert pd.api.types.is_numeric_dtype(df["spend"])
    assert pd.api.types.is_integer_dtype(df["clicks"])


def test_mock_marketing_api_consistent_campaigns():
    """Test that mock API returns consistent number of campaigns."""
    result1 = mock_marketing_api("2024-01-01")
    result2 = mock_marketing_api("2024-01-02")
    
    # Should return same number of campaigns for different dates
    assert len(result1) == len(result2)


def test_mock_marketing_api_valid_platforms():
    """Test that all platforms are from expected set."""
    result = mock_marketing_api("2024-01-01")
    valid_platforms = {"Facebook", "Google", "LinkedIn"}
    
    for record in result:
        assert record["platform"] in valid_platforms


def test_mock_marketing_api_positive_values():
    """Test that spend and clicks are positive."""
    result = mock_marketing_api("2024-01-01")
    
    for record in result:
        assert record["spend"] > 0
        assert record["clicks"] > 0
        assert record["impressions"] > 0


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
    })


@pytest.fixture
def sample_users_df():
    """Sample users DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
    })


def test_extract_table_by_date(mock_db_connection, mock_read_sql, sample_orders_df):
    """Test extracting table filtered by date."""
    mock_read_sql.return_value = sample_orders_df
    
    result = extract_table_by_date("orders", "2025-12-01", "created_at")
    
    # Verify SQL query was called with correct parameters
    mock_read_sql.assert_called_once()
    call_args = mock_read_sql.call_args
    assert "orders" in call_args[0][0]
    assert "created_at::DATE" in call_args[0][0]
    assert call_args[1]["params"] == ("2025-12-01",)
    
    # Verify result
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    mock_db_connection.close.assert_called_once()


def test_extract_orders(mock_read_sql, sample_orders_df, tmp_path):
    """Test extracting orders for specific date."""
    mock_read_sql.return_value = sample_orders_df
    
    # Mock get_data_lake_path
    with patch("scripts.extractors.db_extractor.get_data_lake_path") as mock_path:
        output_path = tmp_path / "orders.parquet"
        mock_path.return_value = output_path
        
        result_path = extract_orders("2025-12-01")
        
        # Verify file was created
        assert result_path == output_path
        assert output_path.exists()
        
        # Verify data in parquet file
        df = pd.read_parquet(output_path)
        assert len(df) == 3
        assert "user_id" in df.columns


def test_extract_order_items_by_orders(mock_read_sql, tmp_path):
    """Test extracting order items for orders on specific date."""
    order_items_df = pd.DataFrame({
        "id": [1, 2, 3],
        "order_id": [100, 101, 102],
        "product_id": [50, 51, 52],
        "quantity": [2, 1, 5],
    })
    mock_read_sql.return_value = order_items_df
    
    with patch("scripts.extractors.db_extractor.get_data_lake_path") as mock_path:
        output_path = tmp_path / "order_items.parquet"
        mock_path.return_value = output_path
        
        result_path = extract_order_items_by_orders("2025-12-01")
        
        # Verify SQL includes JOIN with orders
        call_args = mock_read_sql.call_args
        assert "order_items" in call_args[0][0]
        assert "INNER JOIN orders" in call_args[0][0]
        
        # Verify file was created
        assert result_path == output_path
        assert output_path.exists()
        
        df = pd.read_parquet(output_path)
        assert len(df) == 3
        assert "order_id" in df.columns
        assert "product_id" in df.columns


def test_extract_full_table(mock_read_sql, sample_users_df, tmp_path):
    """Test extracting full table snapshot."""
    mock_read_sql.return_value = sample_users_df
    
    with patch("scripts.extractors.db_extractor.get_data_lake_path") as mock_path:
        output_path = tmp_path / "users.parquet"
        mock_path.return_value = output_path
        
        result_path = extract_full_table("users", "2025-12-01")
        
        # Verify SQL is simple SELECT *
        call_args = mock_read_sql.call_args
        assert "SELECT * FROM users" in call_args[0][0]
        
        # Verify file was created
        assert result_path == output_path
        assert output_path.exists()
        
        df = pd.read_parquet(output_path)
        assert len(df) == 3
        assert "email" in df.columns


def test_extract_orders_empty_result(mock_read_sql, tmp_path):
    """Test extracting orders when no data for date."""
    empty_df = pd.DataFrame(columns=["id", "user_id", "status", "created_at"])
    mock_read_sql.return_value = empty_df
    
    with patch("scripts.extractors.db_extractor.get_data_lake_path") as mock_path:
        output_path = tmp_path / "orders_empty.parquet"
        mock_path.return_value = output_path
        
        result_path = extract_orders("2025-01-01")
        
        # Should still create file even if empty
        assert output_path.exists()
        df = pd.read_parquet(output_path)
        assert len(df) == 0


def test_extract_table_connection_cleanup(mock_db_connection, mock_read_sql, sample_orders_df):
    """Test that database connection is properly closed."""
    mock_read_sql.return_value = sample_orders_df
    
    extract_table_by_date("orders", "2025-12-01")
    
    # Verify connection was closed
    mock_db_connection.close.assert_called_once()


def test_extract_table_connection_cleanup_on_error(mock_db_connection, mock_read_sql):
    """Test that connection is closed even when error occurs."""
    mock_read_sql.side_effect = Exception("Database error")
    
    with pytest.raises(Exception):
        extract_table_by_date("orders", "2025-12-01")
    
    # Connection should still be closed
    mock_db_connection.close.assert_called_once()


def test_extract_orders_parquet_format(mock_read_sql, sample_orders_df, tmp_path):
    """Test that extracted data is saved in correct Parquet format."""
    mock_read_sql.return_value = sample_orders_df
    
    with patch("scripts.extractors.db_extractor.get_data_lake_path") as mock_path:
        output_path = tmp_path / "orders.parquet"
        mock_path.return_value = output_path
        
        extract_orders("2025-12-01")
        
        # Verify file is valid Parquet
        df = pd.read_parquet(output_path)
        assert isinstance(df, pd.DataFrame)
        
        # Verify data integrity
        pd.testing.assert_frame_equal(df, sample_orders_df)
