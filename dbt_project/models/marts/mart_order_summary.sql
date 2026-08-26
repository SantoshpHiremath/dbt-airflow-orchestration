-- Order-management summary mart: aggregates cleaned order data by region
-- and product line for a Power BI-style reporting dashboard — order
-- counts, total/average value, and average processing time, with
-- separate visibility into how many rows had a missing value (so a
-- dashboard consumer can see data-quality gaps rather than have them
-- silently averaged away).

select
    region,
    product_line,
    count(*) as order_count,
    sum(case when is_missing_value then 1 else 0 end) as orders_missing_value,
    round(sum(coalesce(order_value_eur, 0)), 2) as total_order_value_eur,
    round(avg(order_value_eur), 2) as avg_order_value_eur,
    round(avg(processing_days), 1) as avg_processing_days
from {{ ref('stg_orders') }}
group by region, product_line
order by total_order_value_eur desc
