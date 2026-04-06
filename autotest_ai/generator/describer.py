"""
generator/describer.py
Optionally generates human-readable column descriptions using an LLM.
Falls back to sensible rule-based stubs when no API key is available.
"""

from typing import Dict, List
import re


# ── Rule-based fallback ───────────────────────────────────────────────────────

_KEYWORD_DESCRIPTIONS = {
    "_id":        "Unique identifier",
    "_at":        "Timestamp indicating when the event occurred",
    "_date":      "Date of the associated event or record",
    "_name":      "Human-readable name",
    "_status":    "Current status or state",
    "_type":      "Category or type classification",
    "_count":     "Aggregated count",
    "_amount":    "Monetary amount",
    "_price":     "Price value",
    "_flag":      "Boolean indicator flag",
    "is_":        "Boolean flag",
    "has_":       "Boolean flag indicating presence",
    "created_at": "Timestamp when the record was created",
    "updated_at": "Timestamp when the record was last updated",
    "deleted_at": "Timestamp when the record was soft-deleted",
    "email":      "Email address of the associated entity",
    "phone":      "Phone number",
    "address":    "Physical or mailing address",
}


def _rule_based_description(col: str) -> str:
    col_lower = col.lower()
    for kw, desc in _KEYWORD_DESCRIPTIONS.items():
        if kw in col_lower:
            return desc
    # Convert snake_case to readable label as last resort
    return col.replace("_", " ").capitalize()


# ── LLM-powered path (optional) ───────────────────────────────────────────────

def _llm_description(model_name: str, col: str) -> str:
    """
    Real LLM call via Anthropic API.
    Requires ANTHROPIC_API_KEY in env.
    Falls back silently to rule-based.
    """
    try:
        import anthropic  # type: ignore
        client = anthropic.Anthropic()
        prompt = (
            f"You are a data documentation expert.\n"
            f"Model: {model_name}, Column: {col}\n"
            f"Write a single concise sentence (max 15 words) describing what "
            f"this column likely represents. Be specific and professional."
        )
        msg = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=60,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip().rstrip(".")
    except Exception:
        return _rule_based_description(col)


# ── Public API ────────────────────────────────────────────────────────────────

def generate_descriptions(
    model_name:  str,
    columns:     List[str],
    use_llm:     bool = False,
) -> Dict[str, str]:
    """Return {col: description} for every column."""
    fn = _llm_description if use_llm else _rule_based_description
    return {
        col: (fn(model_name, col) if use_llm else _rule_based_description(col))
        for col in columns
    }
