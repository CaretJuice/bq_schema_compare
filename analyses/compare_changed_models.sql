{#-
    CI/CD Schema Comparison Analysis (Reference Implementation)

    This analysis is a reference implementation for CI/CD use cases.
    For better CI/CD integration, consider using the run-operation approach:

        dbt run-operation compare_schemas --args '{
            prod_dataset: "production_analytics",
            compare_dataset: "staging_analytics",
            models: "${CHANGED_MODELS}",
            fail_on_diff: true
        }'

    The run-operation approach is preferred for CI/CD because:
    - Results are logged directly to console (no need to cat compiled SQL)
    - The fail_on_diff option provides a proper exit code for CI gates
    - No separate BigQuery query execution step required

    Variant of compare_schemas designed for CI/CD pipeline integration.
    Accepts models as either a list or comma-separated string for easier shell scripting.

    NOTE: This analysis is skipped (returns empty result) when not configured.
    This allows the package to be installed without blocking dbt runs.

    Usage (compile approach):
        # Get changed models from git diff
        CHANGED_MODELS=$(git diff --name-only origin/main...HEAD -- "*/models/**/*.sql" | \
            xargs -I {} basename {} .sql | sort -u | paste -sd,)

        # Compile with changed models
        dbt compile --select compare_changed_models --vars "{
            bq_schema_compare_prod_dataset: \"production_analytics\",
            bq_schema_compare_compare_dataset: \"staging_analytics\",
            bq_schema_compare_models: \"${CHANGED_MODELS}\"
        }"

    Usage (run-operation approach - recommended):
        CHANGED_MODELS=$(git diff --name-only origin/main...HEAD -- "*/models/**/*.sql" | \
            xargs -I {} basename {} .sql | sort -u | paste -sd,)

        dbt run-operation compare_schemas --args "{
            prod_dataset: \"production_analytics\",
            compare_dataset: \"staging_analytics\",
            models: \"${CHANGED_MODELS}\",
            fail_on_diff: true
        }"

    Or with explicit list:
        dbt compile --select compare_changed_models --vars '{
            bq_schema_compare_prod_dataset: "production_analytics",
            bq_schema_compare_compare_dataset: "staging_analytics",
            bq_schema_compare_models: "fct_orders,dim_customers"
        }'

    Output is identical to compare_schemas.sql
-#}

{%- if not bq_schema_compare.is_configured() -%}
-- bq_schema_compare: Not configured (missing prod_dataset or compare_dataset)
-- Set bq_schema_compare_prod_dataset and bq_schema_compare_compare_dataset to enable
{{ bq_schema_compare.empty_result_set() }}
{%- else -%}

{%- set prod_dataset = var('bq_schema_compare_prod_dataset') -%}
{%- set compare_dataset = var('bq_schema_compare_compare_dataset') -%}
{%- set prod_project = bq_schema_compare.get_effective_project(var('bq_schema_compare_prod_project', '')) -%}
{%- set compare_project = bq_schema_compare.get_effective_project(var('bq_schema_compare_compare_project', '')) -%}
{%- set models = bq_schema_compare.parse_model_list(var('bq_schema_compare_models', [])) -%}

{%- if models | length == 0 -%}
-- bq_schema_compare: No models specified in bq_schema_compare_models variable
-- Pass models as comma-separated string: --vars '{"bq_schema_compare_models": "model1,model2"}'
{{ bq_schema_compare.empty_result_set() }}
{%- else -%}
-- Schema Comparison (CI/CD): {{ prod_project }}.{{ prod_dataset }} vs {{ compare_project }}.{{ compare_dataset }}
-- Changed models: {{ models | join(', ') }}

{{ bq_schema_compare.compare_all_tables(prod_project, prod_dataset, compare_project, compare_dataset, models) }}

order by table_name, column_name
{%- endif -%}
{%- endif -%}
