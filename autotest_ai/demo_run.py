#!/usr/bin/env python3
"""
demo_run.py
─────────────────────────────────────────────────────────────────────────────
AutoTest AI — DEMO MODE (no real database required)
Generates synthetic DataFrames that simulate your dbt model tables,
then runs the full profiling + test-inference + YAML generation pipeline.

Run:
    python demo_run.py
─────────────────────────────────────────────────────────────────────────────
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))

from profiler.discovery  import discover_models, ModelInfo
from profiler.profiler   import profile_dataframe
from rules.engine        import infer_tests
from generator.yaml_gen  import build_schema_yml
from generator.writer    import save_schema_yml
from generator.describer import generate_descriptions


BANNER = """
╔══════════════════════════════════════════════════════╗
║       AutoTest AI  —  DEMO MODE (mock data)          ║
║  Scan → Profile → Infer Tests → Generate schema.yml  ║
╚══════════════════════════════════════════════════════╝
"""

np.random.seed(42)
N = 500   # rows per mock table

# ── Mock data factory ─────────────────────────────────────────────────────────

def mock_stg_orders() -> pd.DataFrame:
    statuses   = ["pending", "shipped", "delivered", "cancelled"]
    return pd.DataFrame({
        "order_id":     range(1, N + 1),
        "customer_id":  np.random.randint(1, 200, N),
        "order_date":   pd.date_range("2024-01-01", periods=N, freq="h").date,
        "status":       np.random.choice(statuses, N),
        "total_amount": np.round(np.random.uniform(5, 500, N), 2),
        "is_returned":  np.random.choice([True, False], N),
        "created_at":   pd.date_range("2024-01-01", periods=N, freq="h"),
        "updated_at":   pd.date_range("2024-01-01", periods=N, freq="h") + pd.Timedelta(hours=1),
    })


def mock_stg_customers() -> pd.DataFrame:
    first_names = ["Alice", "Bob", "Carol", "Dan", "Eve", "Frank", "Grace", "Hank"]
    last_names  = ["Smith", "Jones", "Williams", "Brown", "Davis", "Miller", "Wilson"]
    countries   = ["US", "UK", "DE", "IN", "CA", "AU"]
    plans       = ["free", "pro", "enterprise"]

    df = pd.DataFrame({
        "customer_id":   range(1, N + 1),
        "first_name":    np.random.choice(first_names, N),
        "last_name":     np.random.choice(last_names,  N),
        "email":         [f"user{i}@example.com" for i in range(1, N + 1)],
        "country":       np.random.choice(countries, N),
        "plan_type":     np.random.choice(plans, N),
        "is_active":     np.random.choice([True, False], N, p=[0.9, 0.1]),
        "signup_date":   pd.date_range("2020-01-01", periods=N, freq="D").date,
        "last_login_at": pd.date_range("2024-01-01", periods=N, freq="h"),
    })

    # Introduce 3 % nulls in last_login_at to keep it realistic
    mask = np.random.rand(N) < 0.03
    df.loc[mask, "last_login_at"] = None
    return df


MOCK_DATA = {
    "stg_orders":    mock_stg_orders(),
    "stg_customers": mock_stg_customers(),
}

# ── Pipeline ──────────────────────────────────────────────────────────────────

def run():
    print(BANNER)
    models_dir = Path(__file__).parent / "models"

    models = discover_models(models_dir)
    print(f"🔍  Found {len(models)} model(s): {[m.name for m in models]}\n")

    results = []

    for model in models:
        df = MOCK_DATA.get(model.name)
        if df is None:
            print(f"⚠️   No mock data for '{model.name}' — skipping.\n")
            continue

        print(f"━━━  Model: {model.name}  ({len(df)} rows × {len(df.columns)} cols)  ━━━")

        # Profile
        profiles = profile_dataframe(df)
        print("  Profiles:")
        for col, p in profiles.items():
            print(f"    {col:20s}  null={p['null_pct']:5.1f}%  distinct={p['distinct_count']:4d}  type={p['inferred_type']}")

        # Infer tests
        col_tests, explanations = infer_tests(profiles, model.name)
        print("\n  Inferred tests:")
        for col, tests in col_tests.items():
            if tests:
                print(f"    {col:20s}  → {tests}")

        # Auto-descriptions
        descriptions = generate_descriptions(model.name, list(profiles.keys()))

        results.append({
            "model":        model,
            "profiles":     profiles,
            "tests":        col_tests,
            "explanations": explanations,
            "descriptions": descriptions,
        })
        print()

    # Generate YAML
    schema_doc = build_schema_yml(results)
    out_path   = save_schema_yml(models_dir, schema_doc)

    print(f"\n📝  schema.yml written → {out_path}\n")
    print("─" * 60)
    print(open(out_path).read())
    print("─" * 60)

    # Explanations
    print("\n💡  Test Explanations:")
    for r in results:
        print(f"\n  Model: {r['model'].name}")
        for col, reasons in r["explanations"].items():
            for reason in reasons:
                print(f"    • {reason}")

    print("\n✨  AutoTest AI demo complete!\n")


if __name__ == "__main__":
    run()
