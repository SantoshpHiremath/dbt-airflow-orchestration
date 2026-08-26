-- Custom singular test: after staging, no processing_days value should
-- ever be negative (the staging model is supposed to null these out, not
-- just pass them through). dbt tests pass when the query returns ZERO
-- rows — so this fails loudly if the cleaning logic in stg_orders.sql
-- ever regresses.

select *
from {{ ref('stg_orders') }}
where processing_days < 0
