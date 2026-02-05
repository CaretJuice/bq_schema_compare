{{
    config(
        materialized='table',
        alias='type_mismatch'
    )
}}

-- In prod: amount is INT64
-- In compare: amount is FLOAT64 (type mismatch)
select
    cast(1 as int64) as id,
    cast(100.0 as float64) as amount,
    cast('USD' as string) as currency
