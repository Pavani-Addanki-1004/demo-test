#!/usr/bin/env python3
"""
AutoTest AI — AI-Driven dbt Schema & Test Generator
Automatically scans your dbt models, profiles the data, and writes schema.yml
"""

import argparse
import sys
from pathlib import Path

from profiler.discovery   import discover_models
from profiler.data_loader import load_model_data
from profiler.profiler    import profile_dataframe
from rules.engine         import infer_tests
from generator.yaml_gen   import build_schema_yml
from generator.writer     import save_schema_yml
from generator.describer  import generate_descriptions
from config               import load_config


BANNER = """
╔══════════════════════════════════════════════════════╗
║          AutoTest AI  —  AI-Driven dbt Testing       ║
║     Scans → Profiles → Infers → Generates YAML       ║
╚══════════════════════════════════════════════════════╝
"""


def run(config_path: str = "config.yml", dry_run: bool = False, llm_descriptions: bool = False):
    print(BANNER)
    cfg = load_config(config_path)

    models_dir   = Path(cfg["dbt"]["models_dir"])
    db_url       = cfg["database"]["url"]
    schema       = cfg.get("database", {}).get("schema", "public")
    sample_rows  = cfg.get("profiling", {}).get("sample_rows", 1000)

    # ── Step 1: Discover models ───────────────────────────────────────────────
    print("🔍  Step 1 — Discovering dbt models …")
    models = discover_models(models_dir)
    if not models:
        print("  ⚠️  No .sql files found. Check your models_dir in config.yml")
        sys.exit(1)
    print(f"  ✅  Found {len(models)} model(s): {[m.name for m in models]}\n")

    results = []

    for model in models:
        print(f"━━━  Processing model: {model.name}  ━━━")

        # ── Step 2: Fetch data ────────────────────────────────────────────────
        print(f"  📥  Step 2 — Loading data (LIMIT {sample_rows}) …")
        df = load_model_data(db_url, schema, model.name, sample_rows)
        if df is None or df.empty:
            print(f"  ⚠️  Skipping {model.name} — no data returned.\n")
            continue
        print(f"  ✅  {len(df)} rows × {len(df.columns)} columns loaded.\n")

        # ── Step 3: Profile ───────────────────────────────────────────────────
        print("  📊  Step 3 — Profiling columns …")
        column_profiles = profile_dataframe(df)
        for col, p in column_profiles.items():
            print(f"       {col:25s}  null={p['null_pct']:.1f}%  "
                  f"distinct={p['distinct_count']}  type={p['inferred_type']}")
        print()

        # ── Step 4: Infer tests ───────────────────────────────────────────────
        print("  🧠  Step 4 — Inferring dbt tests …")
        column_tests, explanations = infer_tests(column_profiles, model.name)
        for col, tests in column_tests.items():
            if tests:
                print(f"       {col:25s}  → {tests}")
        print()

        # ── Step 5 (optional): LLM descriptions ──────────────────────────────
        descriptions = {}
        if llm_descriptions:
            print("  💬  Step 5 — Generating column descriptions via LLM …")
            descriptions = generate_descriptions(model.name, list(column_profiles.keys()))
            print()

        results.append({
            "model":         model,
            "profiles":      column_profiles,
            "tests":         column_tests,
            "explanations":  explanations,
            "descriptions":  descriptions,
        })

    # ── Step 5/6: Build & save schema.yml ────────────────────────────────────
    print("📝  Step 5 — Generating schema.yml …")
    schema_doc = build_schema_yml(results)

    if dry_run:
        print("\n──── DRY RUN — schema.yml preview ────")
        print(schema_doc)
        print("──────────────────────────────────────")
    else:
        out_path = save_schema_yml(models_dir, schema_doc)
        print(f"  ✅  schema.yml written → {out_path}\n")

    # ── Print test explanations ───────────────────────────────────────────────
    print("\n💡  Test Explanations:")
    for r in results:
        print(f"\n  Model: {r['model'].name}")
        for col, reasons in r["explanations"].items():
            for reason in reasons:
                print(f"    • {reason}")

    print("\n✨  AutoTest AI complete!\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoTest AI — Auto-generate dbt schema.yml")
    parser.add_argument("--config",       default="config.yml",  help="Path to config.yml")
    parser.add_argument("--dry-run",      action="store_true",   help="Print YAML without saving")
    parser.add_argument("--llm-describe", action="store_true",   help="Generate column descriptions via LLM")
    args = parser.parse_args()

    run(
        config_path=args.config,
        dry_run=args.dry_run,
        llm_descriptions=args.llm_describe,
    )
