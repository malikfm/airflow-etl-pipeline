from pathlib import Path


def get_data_lake_path(table_name: str, execution_date: str) -> Path:
    """
    Generate data lake path for a given table and execution date.
    
    Args:
        table_name: Name of the table (e.g., 'orders', 'marketing')
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path object pointing to the parquet file
        
    Example:
        >>> get_data_lake_path('orders', '2024-01-01')
        PosixPath('data/raw/orders/2024-01-01.parquet')
    """
    base_path = Path("data/raw") / table_name
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path / f"{execution_date}.parquet"
