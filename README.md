# bq_schema_compare

[![CI](https://github.com/CaretJuice/bq_schema_compare/actions/workflows/ci.yml/badge.svg)](https://github.com/CaretJuice/bq_schema_compare/actions/workflows/ci.yml)

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

### Using run-operation (Recommended for CI/CD)

Compare all tables between two datasets:

```bash
dbt run-operation compare_datasets --args '{
    prod_dataset: "production_analytics",
    compare_dataset: "staging_analytics"
}'
```

Compare specific tables:

```bash
dbt run-operation compare_schemas --args '{
    prod_dataset: "production_analytics",
    compare_dataset: "staging_analytics",
    models: "fct_orders,dim_customers"
}'
```

### Using Analyses (Ad-hoc Exploration)

```bash
dbt compile --select compare_datasets --vars '{
    bq_schema_compare_prod_dataset: "production_analytics",
    bq_schema_compare_compare_dataset: "staging_analytics"
}'

# View and run the compiled SQL
cat target/compiled/bq_schema_compare/analyses/compare_datasets.sql
bq query --use_legacy_sql=false < target/compiled/bq_schema_compare/analyses/compare_datasets.sql
```

## Usage Methods

This package supports two usage patterns:

### 1. run-operation (Recommended for CI/CD)

The `dbt run-operation` approach executes the comparison and logs results directly to the console. This is ideal for CI/CD pipelines because:

- Results are logged immediately (no separate query execution step)
- The `fail_on_diff` option provides proper exit codes for CI gates
- Cleaner integration with shell scripts

**Available operations:**

| Operation          | Description                                   |
| ------------------ | --------------------------------------------- |
| `compare_datasets` | Compare ALL tables in both datasets           |
| `compare_schemas`  | Compare specific tables (pass models list)    |

**Example output:**

```
=== BigQuery Schema Comparison ===
Production:  my-project.production_analytics
Comparison:  my-project.staging_analytics

Found 3 difference(s):

Tables only in PRODUCTION (may be deprecated):
  - old_table (last modified: 2024-01-15 10:30:00+00:00)

Column differences:
  - fct_orders.new_column: exists only in comparison (new)
  - dim_customers.customer_id: type mismatch: INT64 → STRING
```

### 2. Analyses (Ad-hoc Exploration)

The analysis files serve as reference implementations and are useful for:

- Ad-hoc exploration in the BigQuery console
- Saving comparison configurations for specific use cases
- Understanding how to call the underlying macros

| Analysis                 | Description                                    |
| ------------------------ | ---------------------------------------------- |
| `compare_datasets`       | Compare ALL tables in both datasets            |
| `compare_schemas`        | Compare specific tables (pass model list)      |
| `compare_changed_models` | CI/CD variant accepting comma-separated string |

## run-operation Reference

### compare_datasets

Compare all tables between two datasets:

```bash
dbt run-operation compare_datasets --args '{
    prod_dataset: "production_analytics",
    compare_dataset: "staging_analytics"
}'
```

With all options:

```bash
dbt run-operation compare_datasets --args '{
    prod_dataset: "production_analytics",
    compare_dataset: "staging_analytics",
    prod_project: "prod-project-id",
    compare_project: "dev-project-id",
    fail_on_diff: true
}'
```

### compare_schemas

Compare specific tables:

```bash
dbt run-operation compare_schemas --args '{
    prod_dataset: "production_analytics",
    compare_dataset: "staging_analytics",
    models: "fct_orders,dim_customers"
}'
```

Models can be passed as a comma-separated string or a list:

```bash
# Comma-separated string
dbt run-operation compare_schemas --args '{models: "table1,table2"}'

# YAML list
dbt run-operation compare_schemas --args '{models: ["table1", "table2"]}'
```

### Operation Parameters

| Parameter         | Required | Default          | Description                                    |
| ----------------- | -------- | ---------------- | ---------------------------------------------- |
| `prod_dataset`    | Yes      | -                | Production dataset name                        |
| `compare_dataset` | Yes      | -                | Comparison dataset name (staging/dev)          |
| `models`          | compare_schemas only | -      | List or comma-separated string of table names  |
| `prod_project`    | No       | `target.project` | Production project ID                          |
| `compare_project` | No       | `target.project` | Comparison project ID                          |
| `fail_on_diff`    | No       | `false`          | Raise error if differences found (for CI gates)|

## CI/CD Integration

### Using run-operation (Recommended)

```bash
# Get changed models from git diff
CHANGED_MODELS=$(git diff --name-only origin/main...HEAD -- "*/models/**/*.sql" | \
    xargs -I {} basename {} .sql | sort -u | paste -sd,)

# Skip if no models changed
if [ -n "$CHANGED_MODELS" ]; then
    dbt run-operation compare_schemas --args "{
        prod_dataset: \"analytics\",
        compare_dataset: \"staging_analytics\",
        models: \"${CHANGED_MODELS}\",
        fail_on_diff: true
    }"
fi
```

### GitHub Actions Example

```yaml
- name: Check schema changes
  run: |
    CHANGED_MODELS=$(git diff --name-only origin/main...HEAD -- "models/**/*.sql" | \
        xargs -I {} basename {} .sql | sort -u | paste -sd,)

    if [ -n "$CHANGED_MODELS" ]; then
      dbt run-operation compare_schemas --args "{
        prod_dataset: 'analytics',
        compare_dataset: 'pr_${{ github.event.pull_request.number }}_analytics',
        models: '${CHANGED_MODELS}',
        fail_on_diff: true
      }"
    fi
```

### Using Analyses (Alternative)

The compile approach is still available but requires an additional step to execute the query:

```bash
CHANGED_MODELS=$(git diff --name-only origin/main...HEAD -- "*/models/**/*.sql" | \
    xargs -I {} basename {} .sql | sort -u | paste -sd,)

if [ -n "$CHANGED_MODELS" ]; then
    dbt compile --select compare_changed_models --vars "{
        bq_schema_compare_prod_dataset: \"analytics\",
        bq_schema_compare_compare_dataset: \"staging_analytics\",
        bq_schema_compare_models: \"${CHANGED_MODELS}\"
    }"

    bq query --use_legacy_sql=false < target/compiled/bq_schema_compare/analyses/compare_changed_models.sql
fi
```

### Using dbt Selectors

```bash
# List models in a specific path
MODELS=$(dbt ls --select "path:models/marts" --output name | paste -sd,)

dbt run-operation compare_schemas --args "{
    prod_dataset: \"analytics\",
    compare_dataset: \"staging_analytics\",
    models: \"${MODELS}\"
}"
```

## Cross-Project Comparison

Compare tables across different BigQuery projects:

```bash
dbt run-operation compare_datasets --args '{
    prod_project: "prod-project-id",
    prod_dataset: "production_analytics",
    compare_project: "dev-project-id",
    compare_dataset: "dev_analytics"
}'
```

## Configuration Variables

When using analyses (not run-operation), configure via `--vars`:

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

## Using Macros Directly

You can also call the underlying macros in your own models or analyses:

```sql
-- In your own analysis or model
{{ bq_schema_compare.compare_datasets_full(
    prod_project='my-project',
    prod_dataset='production',
    compare_project='my-project',
    compare_dataset='staging'
) }}
```

Available macros:

| Macro                   | Description                                    |
| ----------------------- | ---------------------------------------------- |
| `compare_datasets_full` | Returns SQL for full dataset comparison        |
| `compare_all_tables`    | Returns SQL for comparing a list of tables     |
| `compare_table_schemas` | Returns SQL for comparing a single table       |
| `get_schema_columns`    | Returns CTE for table columns from INFORMATION_SCHEMA |

## Notes

- INFORMATION_SCHEMA queries are free in BigQuery
- No tables are materialized - this is analysis-only (inspection, not transformation)
- Empty result set means schemas match perfectly
- When not configured, analyses return empty results with helpful comments

## Development

### Running Tests Locally

1. Set up environment variables:

```bash
export BQ_PROJECT="your-gcp-project"
export BQ_TEST_PROD_DATASET="bq_schema_compare_test_prod"
export BQ_TEST_COMPARE_DATASET="bq_schema_compare_test_compare"
```

2. Create test datasets:

```bash
bq mk --dataset --location=US $BQ_PROJECT:$BQ_TEST_PROD_DATASET
bq mk --dataset --location=US $BQ_PROJECT:$BQ_TEST_COMPARE_DATASET
```

3. Install dependencies and build test tables:

```bash
cd integration_tests
dbt deps
dbt run --select models/prod
dbt run --select models/compare
```

4. Run the integration tests:

```bash
dbt run-operation run_integration_tests
```

5. Cleanup:

```bash
bq rm -r -f $BQ_PROJECT:$BQ_TEST_PROD_DATASET
bq rm -r -f $BQ_PROJECT:$BQ_TEST_COMPARE_DATASET
```

### CI/CD

This project uses GitHub Actions for CI. Tests run automatically on push and pull requests.

Required GitHub secrets:
- `BQ_PROJECT` - GCP project ID for testing
- `BQ_SERVICE_ACCOUNT_JSON` - Service account JSON key with BigQuery permissions

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

MIT License - see [LICENSE](LICENSE) for details.
