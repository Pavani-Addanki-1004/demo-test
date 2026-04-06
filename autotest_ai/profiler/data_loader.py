"""
profiler/data_loader.py
Connects to the database via SQLAlchemy and fetches a sample of each model.
"""

from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text


def load_model_data(
    db_url: str,
    schema: str,
    model_name: str,
    limit: int = 1000,
) -> Optional[pd.DataFrame]:
    """
    Query <schema>.<model_name> and return a DataFrame.
    Returns None if the table doesn't exist or the query fails.
    """
    try:
        engine = create_engine(db_url)
        query  = f'SELECT * FROM "{schema}"."{model_name}" LIMIT {limit}'

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        return df

    except Exception as exc:
        print(f"  ⚠️  Could not load {model_name}: {exc}")
        return None
