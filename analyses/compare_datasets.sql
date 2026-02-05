{#-
    Full Dataset Schema Comparison Analysis (Reference Implementation)

    This analysis is a reference implementation showing how to use the
    bq_schema_compare.compare_datasets_full() macro. For CI/CD integration,
    consider using the run-operation approach instead:

        dbt run-operation compare_datasets --args '{
            prod_dataset: "production_analytics",
            compare_dataset: "staging_analytics"
        }'

    This analysis compares ALL tables between production and comparison datasets.
    Unlike compare_schemas which requires a list of specific models, this analysis
    automatically discovers all tables in both datasets and compares them.

    This is useful for:
    - Finding tables that exist only in production (deprecated, may need deletion)
    - Finding tables that exist only in comparison (new tables being added)
    - Full schema drift detection without specifying individual models

    NOTE: This analysis is skipped (returns empty result) when not configured.
    This allows the package to be installed without blocking dbt runs.

    Usage (compile approach):
        dbt compile --select compare_datasets --vars '{
            bq_schema_compare_prod_dataset: "production_analytics",
            bq_schema_compare_compare_dataset: "staging_analytics"
        }'

        Then run the compiled SQL in BigQuery console:
        cat target/compiled/bq_schema_compare/analyses/compare_datasets.sql

    Usage (run-operation approach - recommended for CI/CD):
        dbt run-operation compare_datasets --args '{
            prod_dataset: "production_analytics",
            compare_dataset: "staging_analytics"
        }'

        # Fail CI on differences:
        dbt run-operation compare_datasets --args '{
            prod_dataset: "production_analytics",
            compare_dataset: "staging_analytics",
            fail_on_diff: true
        }'

    Required variables:
        bq_schema_compare_prod_dataset: Production dataset name
        bq_schema_compare_compare_dataset: Comparison dataset name (staging/dev)

    Optional variables:
        bq_schema_compare_prod_project: Override production project (default: target.project)
        bq_schema_compare_compare_project: Override comparison project (default: target.project)

    Output columns:
        - table_name: Name of the table
        - column_name: Name of the column (null for table-level differences)
        - status: Type of difference:
            - table_prod_only: Table exists only in production (candidate for deletion)
            - table_compare_only: Table exists only in comparison (new table)
            - prod_only: Column exists only in production
            - compare_only: Column exists only in comparison
            - type_mismatch: Column data types differ
            - position_mismatch: Column positions differ
        - prod_data_type: Data type in production
        - compare_data_type: Data type in comparison dataset
        - prod_position: Ordinal position in production
        - compare_position: Ordinal position in comparison
        - prod_last_modified: Last modification timestamp in production
        - compare_last_modified: Last modification timestamp in comparison

    Interpreting Results:
        - table_prod_only with old prod_last_modified: Likely deprecated, safe to delete
        - table_prod_only with recent prod_last_modified: May be actively used, investigate
        - table_compare_only: New table being added in comparison environment
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

-- Full Dataset Comparison: {{ prod_project }}.{{ prod_dataset }} vs {{ compare_project }}.{{ compare_dataset }}
-- Comparing ALL tables in both datasets

{{ bq_schema_compare.compare_datasets_full(prod_project, prod_dataset, compare_project, compare_dataset) }}

order by
    case status
        when 'table_prod_only' then 1
        when 'table_compare_only' then 2
        else 3
    end,
    table_name,
    column_name
{%- endif -%}
