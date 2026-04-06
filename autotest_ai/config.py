"""Config loader — reads config.yml or returns sensible defaults."""

from pathlib import Path
import yaml


DEFAULTS = {
    "dbt": {
        "models_dir": "models",
    },
    "database": {
        "url":    "postgresql://user:password@localhost:5432/mydb",
        "schema": "public",
    },
    "profiling": {
        "sample_rows": 1000,
    },
}


def load_config(path: str = "config.yml") -> dict:
    p = Path(path)
    if not p.exists():
        print(f"  ⚠️  Config file '{path}' not found — using defaults.")
        return DEFAULTS.copy()

    with open(p) as f:
        user_cfg = yaml.safe_load(f) or {}

    # Deep-merge user config over defaults
    cfg = DEFAULTS.copy()
    for section, values in user_cfg.items():
        if isinstance(values, dict):
            cfg.setdefault(section, {}).update(values)
        else:
            cfg[section] = values
    return cfg
