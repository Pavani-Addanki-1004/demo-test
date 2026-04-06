# AutoTest AI Integration

This dbt project is integrated with **AutoTest AI** - an AI-driven testing automation tool.

## What It Does

AutoTest AI automatically:
- Scans all `.sql` models in your project
- Profiles real data from your database
- Infers appropriate dbt tests using AI rules
- Generates production-ready `schema.yml` files

## Quick Start

### 1. Navigate to AutoTest AI
```bash
cd autotest_ai
```

### 2. Run Demo Mode (no database needed)
```bash
python demo_run.py
```

### 3. Run with Real Database
First update `autotest_ai/config.yml` with your database credentials, then:
```bash
python autotest_ai.py                         # Full run
python autotest_ai.py --dry-run               # Preview YAML only
python autotest_ai.py --llm-describe          # Add LLM column descriptions
```

## Generated Tests

The AI automatically infers tests like:

- **ID columns** (`*_id`, `id`) → `not_null`, `unique`
- **Low null percentages** → `not_null`
- **All distinct values** → `unique`
- **Low cardinality columns** → `accepted_values`
- **Boolean columns** (`is_*`, `has_*`) → `accepted_values: [true, false]`
- **Datetime columns** → `not_null`

## Your dbt Models

AutoTest AI discovered these models in your project:
- `models/Marts/Customer/dim_customers.sql`
- `models/Marts/product/dim_products.sql`
- `models/Marts/sales/fct_sales.sql`
- `models/example/my_first_dbt_model.sql`
- `models/example/my_second_dbt_model.sql`
- `models/intermediate/int_orders_enriched.sql`
- `models/staging/stg_customerdata.sql`
- `models/staging/stg_orders.sql`
- `models/staging/stg_productdata.sql`

## Output

The generated `schema.yml` is placed in your `models/` directory and contains automated tests for all discovered models.

## Database Configuration

Update `autotest_ai/config.yml`:
```yaml
database:
  url: snowflake://user:password@account/database/schema?warehouse=WH
  schema: public
```

Supported databases: PostgreSQL, Snowflake, BigQuery, MySQL (any SQLAlchemy-supported database).

## Workflow Integration

1. **Develop** your dbt models as usual
2. **Run** AutoTest AI to generate tests
3. **Review** the generated `schema.yml`
4. **Commit** both models and tests
5. **Run** `dbt test` to validate data quality

## Custom Rules

Add custom test inference rules in `autotest_ai/rules/engine.py` to match your specific data patterns.

---

*AutoTest AI eliminates manual YAML writing and ensures comprehensive data quality testing across your dbt project.*
