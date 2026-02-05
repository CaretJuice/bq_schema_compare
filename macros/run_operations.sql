{#-
    Run-Operation Macros

    These macros are designed to be called via `dbt run-operation` for CI/CD integration.
    They execute the schema comparison queries and log results to the console.

    Usage:
        dbt run-operation compare_datasets --args '{prod_dataset: analytics, compare_dataset: staging}'
        dbt run-operation compare_schemas --args '{prod_dataset: analytics, compare_dataset: staging, models: "table1,table2"}'
-#}


{% macro compare_datasets(prod_dataset=none, compare_dataset=none, prod_project=none, compare_project=none, fail_on_diff=false) %}
{#-
    Compare all tables between two datasets via run-operation.

    Args:
        prod_dataset: Production dataset name (or use var bq_schema_compare_prod_dataset)
        compare_dataset: Comparison dataset name (or use var bq_schema_compare_compare_dataset)
        prod_project: Production project ID (default: target.project)
        compare_project: Comparison project ID (default: target.project)
        fail_on_diff: If true, raise an error when differences are found (useful for CI gates)

    Usage:
        dbt run-operation compare_datasets --args '{prod_dataset: production_analytics, compare_dataset: staging_analytics}'

        # With CI failure on diff:
        dbt run-operation compare_datasets --args '{prod_dataset: analytics, compare_dataset: staging, fail_on_diff: true}'

        # Using project vars instead of args:
        dbt run-operation compare_datasets --vars '{bq_schema_compare_prod_dataset: analytics, bq_schema_compare_compare_dataset: staging}'
-#}

{# Resolve parameters - args take precedence over vars #}
{% set effective_prod_dataset = prod_dataset if prod_dataset else var('bq_schema_compare_prod_dataset', '') %}
{% set effective_compare_dataset = compare_dataset if compare_dataset else var('bq_schema_compare_compare_dataset', '') %}
{% set effective_prod_project = prod_project if prod_project else bq_schema_compare.get_effective_project(var('bq_schema_compare_prod_project', '')) %}
{% set effective_compare_project = compare_project if compare_project else bq_schema_compare.get_effective_project(var('bq_schema_compare_compare_project', '')) %}

{# Validate required parameters #}
{% if not effective_prod_dataset or not effective_compare_dataset %}
    {{ log("ERROR: prod_dataset and compare_dataset are required", info=True) }}
    {{ log("Usage: dbt run-operation compare_datasets --args '{prod_dataset: my_prod, compare_dataset: my_staging}'", info=True) }}
    {{ return(none) }}
{% endif %}

{{ log("", info=True) }}
{{ log("=== BigQuery Schema Comparison ===", info=True) }}
{{ log("Production:  " ~ effective_prod_project ~ "." ~ effective_prod_dataset, info=True) }}
{{ log("Comparison:  " ~ effective_compare_project ~ "." ~ effective_compare_dataset, info=True) }}
{{ log("", info=True) }}

{# Build and execute the comparison query #}
{% set comparison_sql %}
{{ bq_schema_compare.compare_datasets_full(effective_prod_project, effective_prod_dataset, effective_compare_project, effective_compare_dataset) }}
order by
    case status
        when 'table_prod_only' then 1
        when 'table_compare_only' then 2
        else 3
    end,
    table_name,
    column_name
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}
    {{ bq_schema_compare._log_results(results, fail_on_diff) }}
{% endif %}

{% endmacro %}


{% macro compare_schemas(prod_dataset=none, compare_dataset=none, models=none, prod_project=none, compare_project=none, fail_on_diff=false) %}
{#-
    Compare specific tables between two datasets via run-operation.

    Args:
        prod_dataset: Production dataset name (or use var bq_schema_compare_prod_dataset)
        compare_dataset: Comparison dataset name (or use var bq_schema_compare_compare_dataset)
        models: List or comma-separated string of table names (or use var bq_schema_compare_models)
        prod_project: Production project ID (default: target.project)
        compare_project: Comparison project ID (default: target.project)
        fail_on_diff: If true, raise an error when differences are found (useful for CI gates)

    Usage:
        dbt run-operation compare_schemas --args '{prod_dataset: analytics, compare_dataset: staging, models: "fct_orders,dim_customers"}'

        # With CI failure on diff:
        dbt run-operation compare_schemas --args '{prod_dataset: analytics, compare_dataset: staging, models: ["fct_orders"], fail_on_diff: true}'

        # Using project vars:
        dbt run-operation compare_schemas --vars '{bq_schema_compare_prod_dataset: analytics, bq_schema_compare_compare_dataset: staging, bq_schema_compare_models: "model1,model2"}'
-#}

{# Resolve parameters - args take precedence over vars #}
{% set effective_prod_dataset = prod_dataset if prod_dataset else var('bq_schema_compare_prod_dataset', '') %}
{% set effective_compare_dataset = compare_dataset if compare_dataset else var('bq_schema_compare_compare_dataset', '') %}
{% set effective_prod_project = prod_project if prod_project else bq_schema_compare.get_effective_project(var('bq_schema_compare_prod_project', '')) %}
{% set effective_compare_project = compare_project if compare_project else bq_schema_compare.get_effective_project(var('bq_schema_compare_compare_project', '')) %}
{% set models_input = models if models else var('bq_schema_compare_models', []) %}
{% set model_list = bq_schema_compare.parse_model_list(models_input) %}

{# Validate required parameters #}
{% if not effective_prod_dataset or not effective_compare_dataset %}
    {{ log("ERROR: prod_dataset and compare_dataset are required", info=True) }}
    {{ log("Usage: dbt run-operation compare_schemas --args '{prod_dataset: my_prod, compare_dataset: my_staging, models: \"table1,table2\"}'", info=True) }}
    {{ return(none) }}
{% endif %}

{% if model_list | length == 0 %}
    {{ log("ERROR: models parameter is required (list or comma-separated string)", info=True) }}
    {{ log("Usage: dbt run-operation compare_schemas --args '{prod_dataset: my_prod, compare_dataset: my_staging, models: \"table1,table2\"}'", info=True) }}
    {{ return(none) }}
{% endif %}

{{ log("", info=True) }}
{{ log("=== BigQuery Schema Comparison ===", info=True) }}
{{ log("Production:  " ~ effective_prod_project ~ "." ~ effective_prod_dataset, info=True) }}
{{ log("Comparison:  " ~ effective_compare_project ~ "." ~ effective_compare_dataset, info=True) }}
{{ log("Tables:      " ~ model_list | join(", "), info=True) }}
{{ log("", info=True) }}

{# Build and execute the comparison query #}
{% set comparison_sql %}
{{ bq_schema_compare.compare_all_tables(effective_prod_project, effective_prod_dataset, effective_compare_project, effective_compare_dataset, model_list) }}
order by table_name, column_name
{% endset %}

{% if execute %}
    {% set results = run_query(comparison_sql) %}
    {{ bq_schema_compare._log_results(results, fail_on_diff) }}
{% endif %}

{% endmacro %}


{% macro _log_results(results, fail_on_diff=false) %}
{#-
    Internal macro to format and log comparison results.

    Args:
        results: Agate table from run_query
        fail_on_diff: If true, raise an error when differences are found
-#}

{% set diff_count = results | length %}

{% if diff_count == 0 %}
    {{ log("✓ No schema differences found", info=True) }}
    {{ log("", info=True) }}
{% else %}
    {{ log("Found " ~ diff_count ~ " difference(s):", info=True) }}
    {{ log("", info=True) }}

    {# Group by status for cleaner output #}
    {% set table_prod_only = [] %}
    {% set table_compare_only = [] %}
    {% set column_diffs = [] %}

    {% for row in results %}
        {% if row['status'] == 'table_prod_only' %}
            {% do table_prod_only.append(row) %}
        {% elif row['status'] == 'table_compare_only' %}
            {% do table_compare_only.append(row) %}
        {% else %}
            {% do column_diffs.append(row) %}
        {% endif %}
    {% endfor %}

    {# Log table-level differences #}
    {% if table_prod_only | length > 0 %}
        {{ log("Tables only in PRODUCTION (may be deprecated):", info=True) }}
        {% for row in table_prod_only %}
            {{ log("  - " ~ row['table_name'] ~ " (last modified: " ~ row['prod_last_modified'] ~ ")", info=True) }}
        {% endfor %}
        {{ log("", info=True) }}
    {% endif %}

    {% if table_compare_only | length > 0 %}
        {{ log("Tables only in COMPARISON (new tables):", info=True) }}
        {% for row in table_compare_only %}
            {{ log("  - " ~ row['table_name'] ~ " (last modified: " ~ row['compare_last_modified'] ~ ")", info=True) }}
        {% endfor %}
        {{ log("", info=True) }}
    {% endif %}

    {# Log column-level differences #}
    {% if column_diffs | length > 0 %}
        {{ log("Column differences:", info=True) }}
        {% for row in column_diffs %}
            {% set status_desc = {
                'prod_only': 'exists only in production',
                'compare_only': 'exists only in comparison (new)',
                'type_mismatch': 'type mismatch: ' ~ row['prod_data_type'] ~ ' → ' ~ row['compare_data_type'],
                'position_mismatch': 'position changed: ' ~ row['prod_position'] ~ ' → ' ~ row['compare_position']
            } %}
            {{ log("  - " ~ row['table_name'] ~ "." ~ row['column_name'] ~ ": " ~ status_desc.get(row['status'], row['status']), info=True) }}
        {% endfor %}
        {{ log("", info=True) }}
    {% endif %}

    {# Fail if requested #}
    {% if fail_on_diff %}
        {{ exceptions.raise_compiler_error("Schema comparison failed: " ~ diff_count ~ " difference(s) found") }}
    {% endif %}
{% endif %}

{% endmacro %}
