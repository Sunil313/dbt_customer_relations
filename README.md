
# Claims Analytics dbt Models

This project builds a **claims reserves** and **account balances** analytics mart using dbt.
It delivers daily‑grain facts and conformed dimensions to support reporting, trend analysis, and reconciliation.

---

## Table of Contents

- [Architecture](#architecture)
- [Model Overview](#model-overview)
  - [Facts](#facts)
  - [Dimensions](#dimensions)
- [Grain & Keys](#grain--keys)
- [Incremental Strategies](#incremental-strategies)
- [SCD & Time Alignment](#scd--time-alignment)
- [Configuration](#configuration)
- [How to Run](#how-to-run)
- [Testing & Documentation](#testing--documentation)
- [Performance Tips](#performance-tips)
- [Lineage (Conceptual)](#lineage-conceptual)
- [Sample Queries](#sample-queries)

---

## Architecture

- **Staging**: Source standardization (`stg_*` views/tables)
- **Dimensions**: Conformed attributes (claim, policy, customer, product, reserve type, calendar)
- **Facts**:
  - `fct_claim_reserve_snapshots`: Daily reserve snapshots by claim and reserve type
  - `fct_account_balance_daily`: Daily balances by account (running totals from transactions)
- **Macros/Utils**: Surrogate keys, schema naming, parameters (e.g., lookback days)

---

## Model Overview

### Facts

#### `fct_claim_reserve_snapshots`
- **Purpose**: Stores **daily reserve snapshots** per claim and reserve type for trend and point‑in‑time reporting.
- **Upstream**:
  - `stg_claim_reserves_snapshots` (snapshot feed)
  - `fct_claims` (claim ↔ policy relationship)
  - `dim_policy` (policy attributes; ideally SCD2)
- **Important Columns**:
  - `claim_reserve_sk` (surrogate PK)
  - `claim_id`, `policy_id` *(or `policy_sk` if joining SCD2)*
  - `product_code`, `customer_id` *(can be replaced with SKs when dims are wired)*
  - `snapshot_date`, `reserve_type`, `reserve_amount`
  - `load_timestamp`
- **Materialization**: `incremental (append)`
- **Incremental Filter**: Load rows with `reserve_snapshot_date` greater than the current max `snapshot_date` in this table.

#### `fct_account_balance_daily`
- **Purpose**: Computes **daily account balances** from transactions and an opening balance. Useful for EOD balances and reconciliations.
- **Upstream**:
  - `stg_accounts` (opening balance, currency)
  - `dim_calendar` (date spine)
  - `fct_transactions` (signed amounts)
- **Important Columns**:
  - `account_balance_sk` (surrogate PK)
  - `account_id`, `balance_date`
  - `currency_code`
  - `opening_balance`, `closing_balance`, `daily_net_amount`
  - `load_timestamp`
- **Materialization**: `incremental (merge)` with `unique_key = account_balance_sk`
- **Incremental Filter**: Recompute recent days (configurable lookback) to absorb late transactions.

---

### Dimensions

#### `dim_policy`
- **Purpose**: Policy attributes; **recommended SCD2** (has `is_current_record` in your model).
- **Key Columns (typical)**:
  `policy_sk`, `policy_id`, `product_code`, `customer_id`, `effective_from`, `effective_to`, `is_current_record`, …

#### `dim_claim`
- **Purpose**: Claim attributes.
- **Key Columns (example)**:
  `claim_id`, `claim_number`, `loss_date`, `reported_date`, `claim_status`, `cause_of_loss_code`, `currency_code`, …

#### `dim_product`
- **Purpose**: Product master data.
- **Key Columns (example)**:
  `product_code`, `product_name`, `product_line`, `coverage_type`, …

#### `dim_customer`
- **Purpose**: Customer master data (person or organization).
- **Key Columns (example)**:
  `customer_id`, `customer_type`, `first_name`, `last_name`, `organization_name`, `segment`, `country_code`, …

#### `dim_reserve_type`
- **Purpose**: Domain of reserve types (e.g., CASE, EXPENSE, IBNR).
- **Key Columns**:
  `reserve_type_sk`, `reserve_type`

#### `dim_calendar` (a.k.a. `dim_date`)
- **Purpose**: Calendar attributes for date joins.
- **Key Columns**:
  `date_day` (PK), plus Y/M/Q/FY attributes, holiday flags, etc.

---

## Grain & Keys

- **`fct_claim_reserve_snapshots`**:
  **Grain** = `claim_id` × `snapshot_date` × `reserve_type`
  **PK** = `claim_reserve_sk` (surrogate over the 3‑column grain)

- **`fct_account_balance_daily`**:
  **Grain** = `account_id` × `balance_date`
  **PK** = `account_balance_sk` (surrogate over `account_id`,`balance_date`)

- **Dims**:
  - `dim_policy`: **PK** = `policy_sk` (SCD2), **BK** = `policy_id`
  - `dim_claim`: **PK** = `claim_id` (can introduce `claim_sk` if needed)
  - `dim_product`: **PK** = `product_code` (or `product_sk`)
  - `dim_customer`: **PK** = `customer_id` (or `customer_sk` if SCD2)
  - `dim_reserve_type`: **PK** = `reserve_type_sk`
  - `dim_calendar`: **PK** = `date_day`

---

## Incremental Strategies

### `fct_claim_reserve_snapshots` (Append)
```sql
-- Only load new snapshots beyond max existing date
where reserve_snapshot_date > (
  select coalesce(max(snapshot_date), '1900-01-01') from {{ this }}
)
```
- Use **append** if source snapshots never update historically.
- If corrections/late changes can occur, consider:
  - `incremental_strategy='merge'` with `unique_key='claim_reserve_sk'`, or
  - Scheduled periodic `--full-refresh`.

### `fct_account_balance_daily` (Merge)
- Recomputes **recent** daily balances using a **lookback window** to absorb late transactions:
```jinja
{% set lookback_days = var('balances_lookback_days', 14) %}
where transaction_date >= dateadd(day, -1 * {{ lookback_days }}, current_date)
```
- Set `unique_key='account_balance_sk'` for correct merges.

---

## SCD & Time Alignment

- **Policy attributes** in `fct_claim_reserve_snapshots`:
  Avoid joining `is_current_record = true` for historical facts; instead **join to the correct SCD2 version**:
```sql
join {{ ref('dim_policy') }} p
  on c.policy_id = p.policy_id
 and r.snapshot_date >= p.effective_from
 and r.snapshot_date <  coalesce(p.effective_to, '2999-12-31')
```
- Bring **`policy_sk`** into the fact for stable point‑in‑time joins.

---

## Configuration

### `dbt_project.yml` (example)
```yaml
name: claims_analytics
version: 1.0.0
config-version: 2

profile: claims_profile
model-paths: ["models"]
macro-paths: ["macros"]

models:
  claims_analytics:
    staging:
      +schema: staging
      +materialized: view
    marts:
      +schema: analytics
      +materialized: table
```

### Variables (example)
```yaml
vars:
  balances_lookback_days: 14
```

---

## How to Run

```bash
# Install deps (e.g., dbt-utils)
dbt deps

# Build everything
dbt build

# Only facts
dbt run --select marts.facts

# Only dimensions
dbt run --select marts.dimensions

# Run with full refresh for snapshot fact if needed
dbt run --select fct_claim_reserve_snapshots --full-refresh

# Tests & docs
dbt test
dbt docs generate && dbt docs serve
```

---

## Testing & Documentation

Add/extend `schema.yml` in `models/marts/`:

- **Uniqueness/Not Null** on surrogate keys and grains
- **Relationships** to conformed dimensions
- **Accepted Values** for domains like `reserve_type`

Example snippet:
```yaml
version: 2

models:
  - name: fct_claim_reserve_snapshots
    description: "Daily reserve snapshots per claim, date, and reserve type."
    columns:
      - name: claim_reserve_sk
        tests: [unique, not_null]
      - name: snapshot_date
        tests:
          - not_null
          - relationships:
              to: ref('dim_calendar')
              field: date_day
      - name: claim_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_claim')
              field: claim_id
      - name: reserve_type
        tests:
          - relationships:
              to: ref('dim_reserve_type')
              field: reserve_type
```

---

## Performance Tips

- **Partition/Cluster** by date columns (`snapshot_date`, `balance_date`) where supported.
- Keep **lookback windows configurable**; widen during periods of high late‑arriving data.
- For **large joins** to SCD2 dims, ensure appropriate clustering/sorting on effective dates.
- Consider **surrogate keys** (e.g., `policy_sk`) in facts to avoid heavy interval joins at query time.

---

## Lineage (Conceptual)

```
stg_claim_reserves_snapshots ─┐
                              ├─> fct_claim_reserve_snapshots
fct_claims ────────────────────┘            │
                                            ├── joins to dim_policy (SCD2 by snapshot_date)
                                            ├── joins to dim_reserve_type
                                            └── joins to dim_calendar

stg_accounts ─┐
dim_calendar ─┼─> fct_account_balance_daily
fct_transactions ─┘

Additional dims:
stg_claims ──> dim_claim
stg_products ─> dim_product
stg_customers ─> dim_customer
```

---

## Sample Queries

### Reserve Trend by Product (Last 90 Days)
```sql
select
  p.product_code,
  c.date_day as snapshot_date,
  sum(f.reserve_amount) as total_reserve
from analytics.fct_claim_reserve_snapshots f
join analytics.dim_policy p
  on f.policy_id = p.policy_id      -- or f.policy_sk = p.policy_sk (preferred)
join analytics.dim_calendar c
  on f.snapshot_date = c.date_day
where c.date_day >= dateadd(day, -90, current_date)
group by 1, 2
order by 2, 1;
```

### End‑of‑Month Account Balances
```sql
select
  account_id,
  date_trunc('month', balance_date) as month_start,
  any_value(currency_code) as currency_code,
  max_by(closing_balance, balance_date) as eom_closing_balance -- warehouse-specific alt: qualify row_number
from analytics.fct_account_balance_daily
group by 1, 2;
```

---

**Notes**

- Replace `analytics.` with your target schema if different.
- If you introduce SKs for all dims (`policy_sk`, `customer_sk`, `product_sk`, `claim_sk`, `reserve_type_sk`), refactor facts to use SKs and maintain relationships in `schema.yml`.
