{{
    config(
        materialized='table',
        alias='column_diff'
    )
}}

-- In prod: has old_column
-- In compare: has new_column instead (column diff)
select
    cast(1 as int64) as id,
    cast('value' as string) as shared_column,
    cast('new feature' as string) as new_column
