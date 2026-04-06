"""
rules/engine.py
Infers dbt tests from column profiles.

Each rule is a standalone function that receives the column name + profile
and returns a list of (test_dict, explanation_str) tuples.
Add new rules simply by appending to RULES.
"""

from typing import Dict, List, Tuple, Any, Callable


# ── Type aliases ─────────────────────────────────────────────────────────────
Profile     = Dict[str, Any]
TestSpec    = Any          # str OR dict, e.g. "not_null" or {"accepted_values": {...}}
RuleFn      = Callable[[str, Profile], List[Tuple[TestSpec, str]]]


# ── Individual rule functions ─────────────────────────────────────────────────

def rule_id_column(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """Columns whose name ends with '_id' or equals 'id' get not_null + unique."""
    out = []
    name_lower = col.lower()
    if name_lower == "id" or name_lower.endswith("_id"):
        out.append(("not_null", f"{col} → not_null  (column name indicates a primary/foreign key)"))
        out.append(("unique",   f"{col} → unique    (ID columns should have no duplicate values)"))
    return out


def rule_not_null_low_null_pct(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """Columns with < 1 % nulls → not_null."""
    if p["null_pct"] < 1.0:
        return [("not_null", f"{col} → not_null  (only {p['null_pct']:.2f}% null values observed)")]
    return []


def rule_unique_all_distinct(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """Columns where every non-null value is distinct → unique."""
    if p["distinct_count"] == p["row_count"] and p["row_count"] > 0 and p["null_pct"] == 0:
        return [("unique", f"{col} → unique  (all {p['row_count']} values are distinct)")]
    return []


def rule_accepted_values_low_cardinality(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """
    Categorical columns with <= 10 distinct values → accepted_values.
    Skip ID columns and boolean-looking columns (handled by rule_boolean_accepted_values).
    """
    name_lower    = col.lower()
    is_id_col     = name_lower == "id" or name_lower.endswith("_id")
    bool_keywords = {"is_", "has_", "can_", "was_", "did_", "flag"}
    is_bool_name  = any(name_lower.startswith(kw) for kw in bool_keywords)
    is_bool_vals  = set(str(v).lower() for v in p.get("top_values", [])) <= {"true", "false", "1", "0", "yes", "no"}
    is_bool       = is_bool_name or (p["distinct_count"] <= 2 and is_bool_vals)

    if (
        p["inferred_type"] == "categorical"
        and 1 < p["distinct_count"] <= 10
        and not is_id_col
        and not is_bool
    ):
        values = [str(v) for v in p["top_values"]]
        test   = {"accepted_values": {"values": values}}
        expl   = (
            f"{col} → accepted_values  "
            f"(low cardinality: {p['distinct_count']} distinct values → {values})"
        )
        return [(test, expl)]
    return []


def rule_boolean_accepted_values(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """Boolean-looking columns → accepted_values: [true, false]."""
    name_lower = col.lower()
    bool_keywords = {"is_", "has_", "can_", "was_", "did_", "flag"}
    is_bool_name  = any(name_lower.startswith(kw) for kw in bool_keywords)
    is_bool_vals  = set(str(v).lower() for v in p.get("top_values", [])) <= {"true", "false", "1", "0", "yes", "no"}

    if is_bool_name or (p["inferred_type"] == "categorical" and is_bool_vals and p["distinct_count"] <= 2):
        test = {"accepted_values": {"values": ["true", "false"]}}
        return [(test, f"{col} → accepted_values [true, false]  (boolean column detected)")]
    return []


def rule_datetime_not_null(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """Datetime columns with < 5 % nulls → not_null."""
    if p["inferred_type"] == "datetime" and p["null_pct"] < 5.0:
        return [("not_null", f"{col} → not_null  (datetime column with {p['null_pct']:.1f}% nulls)")]
    return []


def rule_numeric_not_null(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    """Numeric columns with < 2 % nulls → not_null."""
    if p["inferred_type"] == "numeric" and p["null_pct"] < 2.0:
        return [("not_null", f"{col} → not_null  (numeric column with {p['null_pct']:.1f}% nulls)")]
    return []


# ── Rule registry — add new rules here ───────────────────────────────────────
RULES: List[RuleFn] = [
    rule_id_column,
    rule_not_null_low_null_pct,
    rule_unique_all_distinct,
    rule_accepted_values_low_cardinality,
    rule_boolean_accepted_values,
    rule_datetime_not_null,
    rule_numeric_not_null,
]


# ── Public API ────────────────────────────────────────────────────────────────

def infer_tests(
    profiles:   Dict[str, Profile],
    model_name: str,
) -> Tuple[Dict[str, List[TestSpec]], Dict[str, List[str]]]:
    """
    Run all rules against each column profile.

    Returns:
        column_tests   – {col: [test, …]}    (deduplicated)
        explanations   – {col: [reason, …]}
    """
    column_tests: Dict[str, List[TestSpec]] = {}
    explanations: Dict[str, List[str]]      = {}

    for col, profile in profiles.items():
        seen_tests: set = set()
        tests: List[TestSpec] = []
        reasons: List[str]    = []

        for rule in RULES:
            for test_spec, explanation in rule(col, profile):
                # Deduplicate by string key
                key = str(test_spec)
                if key not in seen_tests:
                    seen_tests.add(key)
                    tests.append(test_spec)
                    reasons.append(explanation)

        column_tests[col] = tests
        explanations[col] = reasons

    return column_tests, explanations
