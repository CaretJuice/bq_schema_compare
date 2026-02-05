{{
    config(
        materialized='table',
        alias='compare_only'
    )
}}

-- This table only exists in comparison (new table scenario)
select
    cast(1 as int64) as id,
    cast('new feature' as string) as feature_field
