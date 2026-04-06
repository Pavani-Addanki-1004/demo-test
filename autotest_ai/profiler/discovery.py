"""
profiler/discovery.py
Walks the dbt models/ folder and returns a list of ModelInfo objects.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class ModelInfo:
    name: str          # SQL file stem, e.g. "stg_orders"
    sql_path: Path     # absolute path to the .sql file
    subdir: Path       # parent directory (used for scoped schema.yml output)


def discover_models(models_dir: str | Path) -> List[ModelInfo]:
    """
    Recursively find all .sql files under *models_dir*.
    Excludes files starting with '__' (dbt macros / helpers).
    """
    root = Path(models_dir)
    if not root.exists():
        raise FileNotFoundError(f"models_dir not found: {root.resolve()}")

    found = []
    for sql_file in sorted(root.rglob("*.sql")):
        if sql_file.stem.startswith("__"):
            continue
        found.append(
            ModelInfo(
                name=sql_file.stem,
                sql_path=sql_file,
                subdir=sql_file.parent,
            )
        )
    return found
