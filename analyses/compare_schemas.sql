{#-
    Schema Comparison Analysis

    Compares BigQuery table schemas between production and staging/dev datasets.
    Outputs differences including missing columns, type mismatches, and position changes.

    NOTE: This analysis is skipped (returns empty result) when not configured.
    This allows the package to be installed without blocking dbt runs.

    Usage:
        dbt compile --select compare_schemas --vars '{
            bq_schema_compare_prod_dataset: "production_analytics",
            bq_schema_compare_compare_dataset: "staging_analytics",
            bq_schema_compare_models: ["fct_orders", "dim_customers"]
        }'

    Then run the compiled SQL in BigQuery console:
        cat target/compiled/bq_schema_compare/analyses/compare_schemas.sql

    Required variables:
        bq_schema_compare_prod_dataset: Production dataset name
        bq_schema_compare_compare_dataset: Comparison dataset name (staging/dev)
        bq_schema_compare_models: List or comma-separated string of table names

    Optional variables:
        bq_schema_compare_prod_project: Override production project (default: target.project)
        bq_schema_compare_compare_project: Override comparison project (default: target.project)
        bq_schema_compare_region: 'US' or 'EU' (for documentation, not used in query)

    Output columns:
        - table_name: Name of the table being compared
        - column_name: Name of the column with a difference
        - status: Type of difference (prod_only, compare_only, type_mismatch, position_mismatch)
        - prod_data_type: Data type in production (null if column missing)
        - compare_data_type: Data type in comparison dataset (null if column missing)
        - prod_position: Ordinal position in production
        - compare_position: Ordinal position in comparison dataset
        - prod_last_modified: Last modification timestamp in production
        - compare_last_modified: Last modification timestamp in comparison dataset
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
-- bq_schema_compare: No models specified in bq_schema_compare_models
-- Pass models as list: --vars '{"bq_schema_compare_models": ["model1", "model2"]}'
-- Or comma-separated: --vars '{"bq_schema_compare_models": "model1,model2"}'
{{ bq_schema_compare.empty_result_set() }}
{%- else -%}
-- Schema Comparison: {{ prod_project }}.{{ prod_dataset }} vs {{ compare_project }}.{{ compare_dataset }}
-- Tables: {{ models | join(', ') }}

{{ bq_schema_compare.compare_all_tables(prod_project, prod_dataset, compare_project, compare_dataset, models) }}

order by table_name, column_name
{%- endif -%}
{%- endif -%}
