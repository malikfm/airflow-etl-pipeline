from pathlib import Path


def check_file_exists(file_path: str | Path) -> bool:
    """Check if a file exists."""
    return Path(file_path).exists()


def get_data_lake_path(table_name: str, execution_date: str) -> Path:
    """Generate data lake path for a given table and execution date.
    
    Args:
        table_name: Name of the table (e.g., 'orders', 'products')
        execution_date: Date in YYYY-MM-DD format
        
    Returns:
        Path object pointing to the parquet file
        
    Example:
        >>> get_data_lake_path('orders', '2024-01-01')
        PosixPath('data/orders/2024-01-01.parquet')
    """
    base_path = Path("data").joinpath(table_name)
    base_path.mkdir(parents=True, exist_ok=True)
    return base_path / f"{execution_date}.parquet"
