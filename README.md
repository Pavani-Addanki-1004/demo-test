# AutoTest AI — AI-Driven dbt Testing

> Automatically scans your dbt models, profiles real data, infers quality rules,
> and writes a production-ready `schema.yml` — zero manual YAML required.

---

## Project Structure

```
autotest_ai/
├── autotest_ai.py          ← Main CLI (connects to real DB)
├── demo_run.py             ← Demo mode (mock data, no DB needed)
├── config.py               ← Config loader
├── config.yml              ← ⚙️  Edit this with your DB credentials
│
├── profiler/
│   ├── discovery.py        ← Discovers .sql models in models/
│   ├── data_loader.py      ← Queries DB via SQLAlchemy
│   └── profiler.py         ← Computes null %, cardinality, type, min/max
│
├── rules/
│   └── engine.py           ← Rule-based test inference (modular, extensible)
│
├── generator/
│   ├── yaml_gen.py         ← Builds schema.yml document structure
│   ├── writer.py           ← Saves schema.yml to disk
│   └── describer.py        ← Auto column descriptions (rule-based + LLM)
│
└── models/
    ├── stg_orders.sql
    ├── stg_customers.sql
    └── schema.yml          ← ✅ Auto-generated output
```

---

## Quickstart

### Option A — Demo Mode (no DB required)

```bash
cd autotest_ai
python demo_run.py
```

Runs the full pipeline with synthetic data and writes `models/schema.yml`.

---

### Option B — Real Database

**1. Install dependencies**

```bash
pip install pandas pyyaml sqlalchemy psycopg2-binary
# For Snowflake:
pip install snowflake-sqlalchemy
```

**2. Edit `config.yml`**

```yaml
dbt:
  models_dir: models          # path to your dbt models folder

database:
  url: postgresql://user:password@localhost:5432/mydb
  schema: public              # schema where dbt materialises models

profiling:
  sample_rows: 1000
```

**3. Run**

```bash
python autotest_ai.py                         # full run
python autotest_ai.py --dry-run               # preview YAML only
python autotest_ai.py --llm-describe          # add LLM column descriptions
python autotest_ai.py --config path/to/config.yml
```

---

## How It Works

| Step | What happens |
|------|-------------|
| **1 — Discover** | Walks `models/` and finds all `.sql` files |
| **2 — Load** | Queries `LIMIT 1000` rows per model from the DB |
| **3 — Profile** | Computes null %, distinct count, inferred type, min/max per column |
| **4 — Infer** | Rule engine maps column stats → dbt tests |
| **5 — Generate** | Builds valid `schema.yml` with PyYAML |
| **6 — Save** | Writes (or overwrites) `models/schema.yml` |

---

## Inference Rules

| Rule | Trigger | Tests added |
|------|---------|-------------|
| ID column | Name ends with `_id` or equals `id` | `not_null`, `unique` |
| Low nulls | `null_pct < 1%` | `not_null` |
| All distinct | `distinct == row_count` and 0 nulls | `unique` |
| Low cardinality | Categorical, 2–10 distinct values | `accepted_values` |
| Boolean column | Name starts with `is_/has_/can_` or 2-value bool | `accepted_values: [true, false]` |
| Datetime not null | datetime column, `null_pct < 5%` | `not_null` |
| Numeric not null | numeric column, `null_pct < 2%` | `not_null` |

**Adding a new rule** — open `rules/engine.py`, write a function:

```python
def rule_my_custom(col: str, p: Profile) -> List[Tuple[TestSpec, str]]:
    if "price" in col.lower() and p["min_val"] is not None and p["min_val"] < 0:
        return [("dbt_utils.expression_is_true", f"{col} → price should be ≥ 0")]
    return []

RULES.append(rule_my_custom)   # register it
```

---

## Sample Generated `schema.yml`

```yaml
version: 2
models:
- name: stg_customers
  columns:
  - name: customer_id
    description: Unique identifier
    tests:
    - not_null
    - unique
  - name: email
    description: Email address of the associated entity
    tests:
    - not_null
    - unique
  - name: plan_type
    description: Category or type classification
    tests:
    - not_null
    - accepted_values:
        values:
        - free
        - pro
        - enterprise
  - name: is_active
    description: Boolean flag
    tests:
    - not_null
    - accepted_values:
        values:
        - 'true'
        - 'false'

- name: stg_orders
  columns:
  - name: order_id
    description: Unique identifier
    tests:
    - not_null
    - unique
  - name: status
    description: Current status or state
    tests:
    - not_null
    - accepted_values:
        values:
        - pending
        - shipped
        - delivered
        - cancelled
```

---

## Tech Stack

| Library | Purpose |
|---------|---------|
| `pandas` | Data loading and profiling |
| `PyYAML` | YAML generation |
| `SQLAlchemy` | DB-agnostic connection (Postgres, Snowflake, BigQuery…) |
| `anthropic` *(optional)* | LLM column descriptions via `--llm-describe` |

---

## Supported Databases

Any database SQLAlchemy supports. Connection URL examples:

```
# PostgreSQL
postgresql://user:pass@host:5432/dbname

# Snowflake
snowflake://user:pass@account/dbname/schema?warehouse=WH&role=ROLE

# BigQuery
bigquery://project/dataset

# MySQL
mysql+pymysql://user:pass@host:3306/dbname
```

---

*Built for Spaulding Ridge Ideathon 2026 — AutoTest AI team*
