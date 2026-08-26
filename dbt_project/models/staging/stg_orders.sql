-- Staging model: dedupes raw orders (the source has intentional duplicate
-- order_ids to clean), nulls out invalid processing_days rather than
-- silently keeping negative values, and flags rows with a missing
-- order_value so downstream models can decide how to handle them instead
-- of a null quietly propagating through aggregations.

with deduped as (
    select
        *,
        row_number() over (partition by order_id order by order_date) as rn
    from {{ ref('raw_orders') }}
),

cleaned as (
    select
        order_id,
        order_date,
        region,
        product_line,
        status,
        order_value_eur,
        case
            when processing_days < 0 then null
            else processing_days
        end as processing_days,
        (order_value_eur is null) as is_missing_value
    from deduped
    where rn = 1  -- keep only the first occurrence of each order_id
)

select * from cleaned
