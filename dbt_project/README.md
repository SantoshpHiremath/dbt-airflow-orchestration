# Order Management — dbt Data Pipeline

A real, tested dbt project that cleans and models order-management data —
built to get genuine, hands-on experience with dbt (and a SQL-based data
warehouse workflow) since neither appeared in prior projects, closing a
specific gap for postings that ask for dbt/Snowflake experience.

## What this is (read before citing anywhere)

The underlying data (`seeds/raw_orders.csv`, 5,080 rows) is **synthetic —
not real Siemens or any company's order data.** I have no access to real
order-management data, and would never fabricate a claim to it. Instead, I
generated realistic order-management data (order ID, date, region, product
line, status, value, processing time) modeled on a Factory Service Center
workflow, and deliberately injected real data-quality problems into it —
80 duplicate order IDs, 60 missing order values, 25 negative processing-day
values (data-entry errors) — so the dbt models have genuine cleaning work
to do, not a already-clean toy dataset.

Every project, model, and test here is real: it actually runs, against a
real (if synthetic) dataset, with a real DuckDB database file produced by
`dbt run`, and real test failures were possible (and would show up) if the
cleaning logic were wrong.

## Why DuckDB instead of Snowflake

The posting asks for Snowflake specifically. Snowflake requires a paid
cloud account I don't have access to build against. DuckDB is dbt's
in-process, file-based warehouse adapter — same dbt workflow (models,
`ref()`, tests, `dbt run`/`dbt test`/`dbt docs generate`), same SQL
transformation logic, no cloud account needed. This is a real, disclosed
substitution, not a claim of Snowflake experience — the CV and cover
letter say "dbt" and are explicit that the target warehouse here is
DuckDB, not Snowflake.

## Pipeline

- **`seeds/raw_orders.csv`** — synthetic raw order data (5,080 rows,
  intentionally messy).
- **`models/staging/stg_orders.sql`** — deduplicates by `order_id` (keeps
  the first occurrence), nulls out invalid (negative) `processing_days`
  instead of passing bad data through, and flags rows with a missing
  order value.
- **`models/marts/mart_order_summary.sql`** — order count, total/average
  value, and average processing time by region and product line — the
  kind of table a Power BI dashboard would sit on top of.
- **`models/marts/mart_status_funnel.sql`** — order counts and percentage
  share by status (Received → In Progress → Shipped → Delivered, plus
  Cancelled).

## Verification

15 dbt tests, all passing:
- Generic schema tests: `unique`/`not_null` on key columns, `accepted_values`
  on `region` and `status` to catch bad categorical data.
- Two custom singular tests I wrote by hand:
  - `assert_no_negative_processing_days.sql` — fails if any negative
    processing-day value ever makes it past staging (the cleaning logic
    is supposed to null these out).
  - `assert_order_summary_totals_reconcile.sql` — fails if the summed
    order count in the mart doesn't exactly match the row count in
    staging, catching accidental row loss/duplication from the
    `GROUP BY`.

Also confirmed: `dbt seed` loads all 5,080 raw rows; `dbt run` builds all
3 models without error; deduplication verified directly (5,080 raw rows →
5,000 unique orders in `stg_orders`); `dbt docs generate` builds cleanly.

## Running it

```bash
pip install dbt-core dbt-duckdb
dbt seed    # loads raw_orders.csv into DuckDB
dbt run      # builds stg_orders, mart_order_summary, mart_status_funnel
dbt test     # runs all 15 tests
dbt docs generate && dbt docs serve   # browsable model documentation
```
