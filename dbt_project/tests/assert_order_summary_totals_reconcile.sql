-- Custom singular test: the total order_count across mart_order_summary
-- must equal the total row count in stg_orders — catches any accidental
-- row loss or duplication introduced by the group-by in the mart model.

with mart_total as (
    select sum(order_count) as total from {{ ref('mart_order_summary') }}
),
staging_total as (
    select count(*) as total from {{ ref('stg_orders') }}
)

select *
from mart_total, staging_total
where mart_total.total != staging_total.total
