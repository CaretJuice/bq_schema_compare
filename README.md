# bq_schema_compare

A dbt package for comparing BigQuery table schemas between datasets (e.g., production vs staging/dev).

## Installation

Add to your project's `packages.yml`:

```yaml
packages:
  - git: "https://github.com/CaretJuice/bq_schema_compare.git"
    revision: v1.0.0
```

Or for local development:

```yaml
packages:
  - local: ../bq_schema_compare
```

Then run `dbt deps`.

## Permissive by Design

This package is designed to **never block dbt runs**. When variables are not configured, analyses return empty result sets with descriptive comments. This allows you to:

- Install the package in all projects without configuration
- Run `dbt build` without errors even when not using schema comparison
- Selectively enable comparison only when needed

## Quick Start

Compare all tables between two datasets:

```bash
dbt compile --select compare_datasets --vars '{
    bq_schema_compare_prod_dataset: "production_analytics",
    bq_schema_compare_compare_dataset: "staging_analytics"
}'

# View and run the compiled SQL
cat target/compiled/bq_schema_compare/analyses/compare_datasets.sql
bq query --use_legacy_sql=false < target/compiled/bq_schema_compare/analyses/compare_datasets.sql
```

## Analyses Available


| Analysis                 | Description                                    |
| ------------------------ | ---------------------------------------------- |
| `compare_datasets`       | Compare ALL tables in both datasets (default)  |
| `compare_schemas`        | Compare specific tables (pass model list)      |
| `compare_changed_models` | CI/CD variant accepting comma-separated string |


## Usage

### Compare All Tables (Default)

The `compare_datasets` analysis automatically discovers and compares all tables:

```bash
dbt compile --select compare_datasets --vars '{
    bq_schema_compare_prod_dataset: "production_analytics",
    bq_schema_compare_compare_dataset: "staging_analytics"
}'
```

This is useful for:

- Finding deprecated tables that may need deletion
- Discovering new tables in staging
- Full schema drift detection

### Compare Specific Tables

Use `compare_schemas` when you only want to compare certain models:

```bash
dbt compile --select compare_schemas --vars '{
    bq_schema_compare_prod_dataset: "production_analytics",
    bq_schema_compare_compare_dataset: "staging_analytics",
    bq_schema_compare_models: ["fct_orders", "dim_customers"]
}'
```

### Cross-Project Comparison

Compare tables across different BigQuery projects:

```bash
dbt compile --select compare_datasets --vars '{
    bq_schema_compare_prod_project: "prod-project-id",
    bq_schema_compare_prod_dataset: "production_analytics",
    bq_schema_compare_compare_project: "dev-project-id",
    bq_schema_compare_compare_dataset: "dev_analytics"
}'
```

## CI/CD Integration

The `compare_changed_models` analysis accepts a comma-separated string for easier shell scripting:

```bash
# Get changed models from git diff
CHANGED_MODELS=$(git diff --name-only origin/main...HEAD -- "*/models/**/*.sql" | \
    xargs -I {} basename {} .sql | sort -u | paste -sd,)

# Skip if no models changed
if [ -n "$CHANGED_MODELS" ]; then
    dbt compile --select compare_changed_models --vars "{
        bq_schema_compare_prod_dataset: \"analytics\",
        bq_schema_compare_compare_dataset: \"staging_analytics\",
        bq_schema_compare_models: \"${CHANGED_MODELS}\"
    }"

    bq query --use_legacy_sql=false < target/compiled/bq_schema_compare/analyses/compare_changed_models.sql
fi
```

### Alternative: Using dbt Selectors

```bash
# List models in a specific path
MODELS=$(dbt ls --select "path:models/marts" --output name | paste -sd,)

dbt compile --select compare_schemas --vars "{
    bq_schema_compare_prod_dataset: \"analytics\",
    bq_schema_compare_compare_dataset: \"staging_analytics\",
    bq_schema_compare_models: \"${MODELS}\"
}"
```

## Configuration Variables


| Variable                            | Required            | Default          | Description                                   |
| ----------------------------------- | ------------------- | ---------------- | --------------------------------------------- |
| `bq_schema_compare_prod_dataset`    | Yes                 | -                | Production dataset name                       |
| `bq_schema_compare_compare_dataset` | Yes                 | -                | Comparison dataset name (staging/dev)         |
| `bq_schema_compare_models`          | For compare_schemas | `[]`             | List or comma-separated string of table names |
| `bq_schema_compare_prod_project`    | No                  | `target.project` | Production project ID                         |
| `bq_schema_compare_compare_project` | No                  | `target.project` | Comparison project ID                         |
| `bq_schema_compare_region`          | No                  | `US`             | Region for documentation (`US` or `EU`)       |


## Output

The query returns rows only for differences:


| Column                  | Description                                      |
| ----------------------- | ------------------------------------------------ |
| `table_name`            | Name of the table being compared                 |
| `column_name`           | Name of the column (null for table-level status) |
| `status`                | Type of difference (see below)                   |
| `prod_data_type`        | Data type in production                          |
| `compare_data_type`     | Data type in comparison dataset                  |
| `prod_position`         | Ordinal position in production                   |
| `compare_position`      | Ordinal position in comparison                   |
| `prod_last_modified`    | Last modification timestamp in production        |
| `compare_last_modified` | Last modification timestamp in comparison        |


### Status Values

**Table-level (from compare_datasets):**

- `table_prod_only` - Table exists only in production (may be deprecated)
- `table_compare_only` - Table exists only in comparison (new table)

**Column-level:**

- `prod_only` - Column exists only in production
- `compare_only` - Column exists only in comparison dataset (new column)
- `type_mismatch` - Column exists in both but with different data types
- `position_mismatch` - Column exists in both with same type but different positions

### Interpreting table_prod_only Results

When a table exists only in production, check `prod_last_modified`:

- **Old timestamp** (weeks/months ago): Likely deprecated, safe to delete
- **Recent timestamp**: May be actively used, investigate before deleting

## Notes

- INFORMATION_SCHEMA queries are free in BigQuery
- No tables are materialized - this is analysis-only (inspection, not transformation)
- Empty result set means schemas match perfectly
- When not configured, analyses return empty results with helpful comments

