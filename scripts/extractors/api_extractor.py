import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from scripts.common.file_utils import get_data_lake_path

# Seed for reproducibility
random.seed(42)


def mock_marketing_api(execution_date: str) -> List[Dict[str, Any]]:
    """
    Simulate marketing API that returns campaign spend data.
    
    In a real scenario, this would call an external API endpoint.
    For simulation, we generate realistic marketing data.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        List of dictionaries containing marketing campaign data
    """
    campaigns = [
        {"id": "CMP-001", "name": "Facebook Brand Awareness", "platform": "Facebook"},
        {"id": "CMP-002", "name": "Google Search Ads", "platform": "Google"},
        {"id": "CMP-003", "name": "Instagram Stories", "platform": "Facebook"},
        {"id": "CMP-004", "name": "Google Display Network", "platform": "Google"},
        {"id": "CMP-005", "name": "LinkedIn B2B", "platform": "LinkedIn"},
    ]
    
    marketing_data = []
    
    for campaign in campaigns:
        # Simulate varying spend and performance
        spend = round(random.uniform(100.0, 5000.0), 2)
        clicks = random.randint(50, 2000)
        impressions = clicks * random.randint(10, 50)
        
        marketing_data.append({
            "campaign_id": campaign["id"],
            "campaign_name": campaign["name"],
            "date": execution_date,
            "platform": campaign["platform"],
            "spend": spend,
            "clicks": clicks,
            "impressions": impressions,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0.0,
        })
    
    return marketing_data


def json_to_dataframe(json_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert JSON data from API to pandas DataFrame.
    
    Args:
        json_data: List of dictionaries from API response
        
    Returns:
        DataFrame with marketing data
        
    Raises:
        ValueError: If json_data is empty or invalid
    """
    if not json_data:
        raise ValueError("JSON data is empty")
    
    df = pd.DataFrame(json_data)
    
    # Ensure date column is datetime type
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    
    return df


def extract_marketing_data(execution_date: str) -> Path:
    """
    Extract marketing data from mock API and save as Parquet.
    
    Args:
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path to the saved Parquet file
    """
    # Call mock API
    json_data = mock_marketing_api(execution_date)
    
    # Convert to DataFrame
    df = json_to_dataframe(json_data)
    
    print(f"Extracted {len(df)} marketing campaigns for {execution_date}")
    
    # Save to data lake
    output_path = get_data_lake_path("marketing", execution_date)
    df.to_parquet(output_path, index=False, engine="pyarrow")
    
    print(f"Saved {len(df)} marketing records to {output_path}")
    return output_path
