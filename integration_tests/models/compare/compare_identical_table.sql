{{
    config(
        materialized='table',
        alias='identical_table'
    )
}}

-- Table with schema that matches exactly in both datasets
select
    cast(1 as int64) as id,
    cast('test' as string) as name,
    cast(current_timestamp() as timestamp) as created_at
