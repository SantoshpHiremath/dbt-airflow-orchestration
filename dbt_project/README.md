# Order Management: dbt Data Pipeline

A tested dbt project that cleans and models order-management data. I built it for hands-on experience with dbt and a SQL-based data warehouse workflow.

## Data

The underlying data (`seeds/raw_orders.csv`, 5,080 rows) is synthetic. I generated realistic order-management data (order ID, date, region, product line, status, value, processing time) modeled on a Factory Service Center workflow, and deliberately injected data-quality problems into it (80 duplicate order IDs, 60 missing order values, 25 negative processing-day values from data-entry errors) so the dbt models have real cleaning work to do.

Every model and test here runs against that dataset, with a DuckDB database file produced by `dbt run`, and a test would fail if the cleaning logic were wrong.

## Why DuckDB

DuckDB is dbt's in-process, file-based warehouse adapter. It gives the same dbt workflow (models, `ref()`, tests, `dbt run`/`dbt test`/`dbt docs generate`) and the same SQL transformation logic without needing a cloud account. A cloud warehouse such as Snowflake can be used as the target by changing the dbt adapter and profile.

## Pipeline

- **`seeds/raw_orders.csv`**: synthetic raw order data (5,080 rows, intentionally messy).
- **`models/staging/stg_orders.sql`**: deduplicates by `order_id` (keeps the first occurrence), nulls out invalid (negative) `processing_days` instead of passing bad data through, and flags rows with a missing order value.
- **`models/marts/mart_order_summary.sql`**: order count, total/average value, and average processing time by region and product line, the kind of table a Power BI dashboard would sit on top of.
- **`models/marts/mart_status_funnel.sql`**: order counts and percentage share by status (Received → In Progress → Shipped → Delivered, plus Cancelled).

## Tests

15 dbt tests, all passing:
- Generic schema tests: `unique`/`not_null` on key columns, `accepted_values` on `region` and `status` to catch bad categorical data.
- Two custom singular tests I wrote by hand:
  - `assert_no_negative_processing_days.sql` fails if any negative processing-day value makes it past staging (the cleaning logic nulls these out).
  - `assert_order_summary_totals_reconcile.sql` fails if the summed order count in the mart doesn't exactly match the row count in staging, catching accidental row loss/duplication from the `GROUP BY`.

Also confirmed: `dbt seed` loads all 5,080 raw rows; `dbt run` builds all 3 models without error; deduplication verified directly (5,080 raw rows → 5,000 unique orders in `stg_orders`); `dbt docs generate` builds cleanly.

## Running it

```bash
pip install dbt-core dbt-duckdb
dbt seed    # loads raw_orders.csv into DuckDB
dbt run      # builds stg_orders, mart_order_summary, mart_status_funnel
dbt test     # runs all 15 tests
dbt docs generate && dbt docs serve   # browsable model documentation
```
