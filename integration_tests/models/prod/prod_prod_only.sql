{{
    config(
        materialized='table',
        alias='prod_only'
    )
}}

-- This table only exists in production (deprecated table scenario)
select
    cast(1 as int64) as id,
    cast('legacy data' as string) as legacy_field
