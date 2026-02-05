{% macro get_schema_columns(project, dataset, table_name) %}
{#-
    Returns a CTE that queries INFORMATION_SCHEMA.COLUMNS for a specific table.

    Args:
        project: BigQuery project ID
        dataset: BigQuery dataset name
        table_name: Table name to get schema for

    Returns:
        SQL CTE selecting column_name, data_type, ordinal_position
-#}
select
    column_name,
    data_type,
    ordinal_position
from `{{ project }}`.`{{ dataset }}`.INFORMATION_SCHEMA.COLUMNS
where table_name = '{{ table_name }}'
{% endmacro %}


{% macro compare_table_schemas(prod_project, prod_dataset, compare_project, compare_dataset, table_name) %}
{#-
    Compares schema between two tables in different datasets.

    Args:
        prod_project: Production BigQuery project ID
        prod_dataset: Production dataset name
        compare_project: Comparison BigQuery project ID
        compare_dataset: Comparison dataset name (staging/dev)
        table_name: Table name to compare

    Returns:
        SQL query that outputs schema differences with columns:
        - table_name
        - column_name
        - status (prod_only, compare_only, type_mismatch, position_mismatch)
        - prod_data_type
        - compare_data_type
        - prod_position
        - compare_position
        - prod_last_modified
        - compare_last_modified
-#}
with prod_schema as (
    {{ bq_schema_compare.get_schema_columns(prod_project, prod_dataset, table_name) }}
),

compare_schema as (
    {{ bq_schema_compare.get_schema_columns(compare_project, compare_dataset, table_name) }}
),

prod_meta as (
    select timestamp_millis(last_modified_time) as last_modified
    from `{{ prod_project }}`.`{{ prod_dataset }}`.`__TABLES__`
    where table_id = '{{ table_name }}'
),

compare_meta as (
    select timestamp_millis(last_modified_time) as last_modified
    from `{{ compare_project }}`.`{{ compare_dataset }}`.`__TABLES__`
    where table_id = '{{ table_name }}'
),

full_join_schemas as (
    select
        coalesce(p.column_name, c.column_name) as column_name,
        p.data_type as prod_data_type,
        c.data_type as compare_data_type,
        p.ordinal_position as prod_position,
        c.ordinal_position as compare_position,
        case
            when p.column_name is null then 'compare_only'
            when c.column_name is null then 'prod_only'
            when p.data_type != c.data_type then 'type_mismatch'
            when p.ordinal_position != c.ordinal_position then 'position_mismatch'
            else 'match'
        end as status
    from prod_schema p
    full outer join compare_schema c
        on p.column_name = c.column_name
)

select
    '{{ table_name }}' as table_name,
    column_name,
    status,
    prod_data_type,
    compare_data_type,
    prod_position,
    compare_position,
    (select last_modified from prod_meta) as prod_last_modified,
    (select last_modified from compare_meta) as compare_last_modified
from full_join_schemas
where status != 'match'
{% endmacro %}


{% macro compare_all_tables(prod_project, prod_dataset, compare_project, compare_dataset, table_list) %}
{#-
    Compares schemas for multiple tables and unions the results.

    Args:
        prod_project: Production BigQuery project ID
        prod_dataset: Production dataset name
        compare_project: Comparison BigQuery project ID
        compare_dataset: Comparison dataset name (staging/dev)
        table_list: List of table names to compare

    Returns:
        SQL query with UNION ALL of all table comparisons.
        If table_list is empty, returns empty result set.
-#}
{% if table_list | length == 0 %}
{{ bq_schema_compare.empty_result_set() }}
{% elif table_list | length == 1 %}
{# Single table - no need for UNION ALL wrapper #}
{{ bq_schema_compare.compare_table_schemas(prod_project, prod_dataset, compare_project, compare_dataset, table_list[0]) }}
{% else %}
{# Multiple tables - wrap each in subquery for UNION ALL compatibility with CTEs #}
{% for table_name in table_list %}
select * from (
{{ bq_schema_compare.compare_table_schemas(prod_project, prod_dataset, compare_project, compare_dataset, table_name) }}
)
{% if not loop.last %}
union all

{% endif %}
{% endfor %}
{% endif %}
{% endmacro %}


{% macro compare_datasets_full(prod_project, prod_dataset, compare_project, compare_dataset) %}
{#-
    Compares all tables between two datasets, including:
    - Tables that exist only in production (may need deletion)
    - Tables that exist only in comparison dataset (new tables)
    - Schema differences for tables that exist in both

    Args:
        prod_project: Production BigQuery project ID
        prod_dataset: Production dataset name
        compare_project: Comparison BigQuery project ID
        compare_dataset: Comparison dataset name (staging/dev)

    Returns:
        SQL query with comprehensive comparison including table-level and column-level differences.
        Status values:
        - table_prod_only: Table exists only in production (candidate for deletion)
        - table_compare_only: Table exists only in comparison (new table)
        - prod_only: Column exists only in production
        - compare_only: Column exists only in comparison
        - type_mismatch: Column data types differ
        - position_mismatch: Column positions differ
-#}
with prod_tables as (
    {{ bq_schema_compare.get_all_tables(prod_project, prod_dataset) }}
),

compare_tables as (
    {{ bq_schema_compare.get_all_tables(compare_project, compare_dataset) }}
),

table_comparison as (
    select
        coalesce(p.table_name, c.table_name) as table_name,
        case
            when p.table_name is null then 'table_compare_only'
            when c.table_name is null then 'table_prod_only'
            else 'both'
        end as table_status,
        p.last_modified as prod_last_modified,
        c.last_modified as compare_last_modified
    from prod_tables p
    full outer join compare_tables c
        on p.table_name = c.table_name
),

-- Tables only in one dataset (no column-level comparison possible)
table_only_differences as (
    select
        table_name,
        '(entire table)' as column_name,
        table_status as status,
        cast(null as string) as prod_data_type,
        cast(null as string) as compare_data_type,
        cast(null as int64) as prod_position,
        cast(null as int64) as compare_position,
        prod_last_modified,
        compare_last_modified
    from table_comparison
    where table_status != 'both'
),

-- Column-level comparison for tables that exist in both
prod_columns as (
    select
        table_name,
        column_name,
        data_type,
        ordinal_position
    from `{{ prod_project }}`.`{{ prod_dataset }}`.INFORMATION_SCHEMA.COLUMNS
),

compare_columns as (
    select
        table_name,
        column_name,
        data_type,
        ordinal_position
    from `{{ compare_project }}`.`{{ compare_dataset }}`.INFORMATION_SCHEMA.COLUMNS
),

column_comparison as (
    select
        coalesce(p.table_name, c.table_name) as table_name,
        coalesce(p.column_name, c.column_name) as column_name,
        case
            when p.column_name is null then 'compare_only'
            when c.column_name is null then 'prod_only'
            when p.data_type != c.data_type then 'type_mismatch'
            when p.ordinal_position != c.ordinal_position then 'position_mismatch'
            else 'match'
        end as status,
        p.data_type as prod_data_type,
        c.data_type as compare_data_type,
        p.ordinal_position as prod_position,
        c.ordinal_position as compare_position
    from prod_columns p
    full outer join compare_columns c
        on p.table_name = c.table_name
        and p.column_name = c.column_name
    where coalesce(p.table_name, c.table_name) in (
        select table_name from table_comparison where table_status = 'both'
    )
),

column_differences as (
    select
        cc.table_name,
        cc.column_name,
        cc.status,
        cc.prod_data_type,
        cc.compare_data_type,
        cc.prod_position,
        cc.compare_position,
        tc.prod_last_modified,
        tc.compare_last_modified
    from column_comparison cc
    join table_comparison tc on cc.table_name = tc.table_name
    where cc.status != 'match'
)

select * from table_only_differences
union all
select * from column_differences
{% endmacro %}
