import pandas as pd
import pytest

from scripts.extractors.api_extractor import json_to_dataframe, mock_marketing_api


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
