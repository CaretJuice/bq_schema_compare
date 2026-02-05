{{
    config(
        materialized='table',
        alias='type_mismatch'
    )
}}

-- In prod: amount is INT64
-- In compare: amount will be FLOAT64
select
    cast(1 as int64) as id,
    cast(100 as int64) as amount,
    cast('USD' as string) as currency
