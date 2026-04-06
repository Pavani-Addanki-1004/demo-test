"""
profiler/profiler.py
Computes per-column statistics used by the rule engine.
"""

from typing import Dict, Any
import pandas as pd


def _infer_type(series: pd.Series) -> str:
    """Classify a column into one of: numeric, datetime, categorical, text."""
    if pd.api.types.is_bool_dtype(series):
        return "categorical"
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    # Try datetime parse on a small sample
    if series.dtype == object:
        sample = series.dropna().head(50)
        try:
            pd.to_datetime(sample, infer_datetime_format=True)
            # Only call it datetime if most values parsed cleanly
            return "datetime"
        except Exception:
            pass
        # Heuristic: average string length > 40 → free text
        avg_len = sample.astype(str).str.len().mean()
        if avg_len > 40:
            return "text"
    return "categorical"


def profile_dataframe(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Return a dict  {column_name: {stats…}}  for every column in *df*.

    Stats produced:
        null_pct       – percentage of null values (0-100)
        distinct_count – number of unique non-null values
        row_count      – total rows
        inferred_type  – numeric | datetime | categorical | text
        min_val        – min (numeric / datetime only)
        max_val        – max (numeric / datetime only)
        top_values     – list of up to 10 most-frequent values (categorical)
    """
    n_rows = len(df)
    profiles: Dict[str, Dict[str, Any]] = {}

    for col in df.columns:
        series = df[col]
        null_count = series.isna().sum()
        null_pct   = (null_count / n_rows * 100) if n_rows else 0.0
        non_null   = series.dropna()
        distinct   = non_null.nunique()
        col_type   = _infer_type(series)

        profile: Dict[str, Any] = {
            "null_pct":       round(null_pct, 2),
            "null_count":     int(null_count),
            "distinct_count": int(distinct),
            "row_count":      n_rows,
            "inferred_type":  col_type,
            "min_val":        None,
            "max_val":        None,
            "top_values":     [],
        }

        if col_type == "numeric" and not non_null.empty:
            profile["min_val"] = float(non_null.min())
            profile["max_val"] = float(non_null.max())

        elif col_type == "datetime" and not non_null.empty:
            parsed = pd.to_datetime(non_null, errors="coerce").dropna()
            if not parsed.empty:
                profile["min_val"] = str(parsed.min().date())
                profile["max_val"] = str(parsed.max().date())

        elif col_type == "categorical" and not non_null.empty:
            profile["top_values"] = (
                non_null.value_counts().head(10).index.tolist()
            )

        profiles[col] = profile

    return profiles
