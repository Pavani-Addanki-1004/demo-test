"""
generator/yaml_gen.py
Converts profiling + test results into a valid dbt schema.yml string.
"""

from typing import List, Dict, Any
import yaml


def _build_column_node(
    col_name:    str,
    tests:       List[Any],
    description: str = "",
) -> Dict[str, Any]:
    node: Dict[str, Any] = {"name": col_name}
    if description:
        node["description"] = description
    if tests:
        node["tests"] = tests
    return node


def build_schema_yml(results: List[Dict[str, Any]]) -> str:
    """
    Build the full schema.yml document string from a list of result dicts.

    Each result dict must contain:
        model        – ModelInfo
        tests        – {col: [test_spec, …]}
        descriptions – {col: str}   (may be empty)
    """
    models_list = []

    for r in results:
        model_name   = r["model"].name
        column_tests = r["tests"]
        descriptions = r.get("descriptions", {})

        columns = []
        for col, tests in column_tests.items():
            desc = descriptions.get(col, "")
            columns.append(_build_column_node(col, tests, desc))

        model_node: Dict[str, Any] = {"name": model_name, "columns": columns}
        models_list.append(model_node)

    doc = {"version": 2, "models": models_list}

    # Use PyYAML with nice formatting
    return yaml.dump(
        doc,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
        indent=2,
    )
