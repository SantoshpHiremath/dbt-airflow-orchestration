-- Order-status funnel mart: counts and percentage share of orders at
-- each status, for a process-digitalization view of where orders are
-- currently sitting in the pipeline (Received -> In Progress -> Shipped
-- -> Delivered, plus Cancelled).

with totals as (
    select count(*) as total_orders from {{ ref('stg_orders') }}
)

select
    s.status,
    count(*) as order_count,
    round(100.0 * count(*) / t.total_orders, 1) as pct_of_total
from {{ ref('stg_orders') }} s
cross join totals t
group by s.status, t.total_orders
order by order_count desc
