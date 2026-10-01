# Technical Documentation — Superstore Data Warehouse

> This document covers the internals of the project: data lineage, the data dictionary, transformation logic, orchestration, testing, operations and known limitations. For a quick overview and setup, see the [README](../README.md).

## Contents

1. [Overview](#1-overview)
2. [Source Data](#2-source-data)
3. [Data Lineage](#3-data-lineage)
4. [Layer Details](#4-layer-details)
5. [Data Dictionary](#5-data-dictionary)
6. [Orchestration](#6-orchestration)
7. [Data Quality & Testing](#7-data-quality--testing)
8. [Infrastructure](#8-infrastructure)
9. [Configuration Reference](#9-configuration-reference)
10. [Operations Runbook](#10-operations-runbook)
11. [Troubleshooting](#11-troubleshooting)
12. [Known Limitations](#12-known-limitations)

---

## 1. Overview

| Item              | Value                                                   |
|-------------------|---------------------------------------------------------|
| Pattern           | Batch ELT, full refresh                                 |
| Orchestrator      | Apache Airflow 3.1 (standalone mode, Python 3.11)       |
| Transformation    | dbt-core 1.12.5 with dbt-duckdb 1.11.0                  |
| Storage / compute | DuckDB 1.5.6, a single embedded file (`my_project/dev.duckdb`) |
| Modeling approach | Kimball dimensional model (star schema)                 |
| Schedule          | Daily (`@daily`, UTC)                                   |
| Runtime           | One Docker container                                    |

**Business questions it answers:** sales and profit by product category, customer segment, region and time period; shipping lead time (order date to ship date); discount impact on profitability.

---

## 2. Source Data

| Property     | Value                                  |
|--------------|----------------------------------------|
| File         | `data/raw/Superstore.xlsx`             |
| Sheet        | `Sample - Superstore`                  |
| Rows         | 9,994 order line items                 |
| Orders       | 5,009 distinct `Order ID`s             |
| Date range   | Orders 2014-01-03 → 2017-12-30, shipments through 2018-01-05 |
| Totals       | ≈ 2.30 M sales, ≈ 286 K profit          |
| Grain        | One row per product per order          |

**Known source quirk:** a single `Product ID` can appear with more than one `Product Name`. The data model works around this (see [dim_product](#dim_product)).

---

## 3. Data Lineage

```
Superstore.xlsx
      │  load_to_ods.py  (DuckDB read_xlsx)
      ▼
ods.superstore ─────────────── source('ods', 'superstore')
      │
      ▼
main_stg.stg_superstore  (view)
      │
      ├──▶ main_dwh.dim_customer  ──┐
      ├──▶ main_dwh.dim_product   ──┤
      ├──▶ main_dwh.dim_location  ──┼──▶ main_dwh.fact_orders
      └──▶ main_dwh.dim_date      ──┘     (joined twice: order & ship date)
```

Run `dbt docs generate && dbt docs serve` from `my_project/` to see the interactive version of this graph.

---

## 4. Layer Details

### 4.1 ODS — `ods.superstore`

- **Built by:** [`scripts/load_to_ods.py`](../scripts/load_to_ods.py)
- **Method:** `CREATE OR REPLACE TABLE … AS SELECT * FROM read_xlsx(...)` using DuckDB's `excel` extension
- **Behavior:** full reload on every run, so it is idempotent and safe to re-run
- **Columns:** kept exactly as in the spreadsheet, including spaces in names (e.g. `"Order ID"`)

### 4.2 Staging — `main_stg.stg_superstore`

- **Materialization:** view
- **Transformations:**
  - Renames every column to `snake_case`
  - Casts `Order Date` / `Ship Date` to `DATE`
  - No filtering or aggregation, so the grain matches the source (9,994 rows)
- **Purpose:** the single cleaned interface that every mart model reads from. No mart model reads the ODS directly.

### 4.3 Marts — `main_dwh.*`

- **Materialization:** table (rebuilt on every `dbt run`)
- **Surrogate keys:** `md5()` hashes of the natural or business key, so keys are deterministic across rebuilds

---

## 5. Data Dictionary

### `fact_orders`

Grain: **one row per order line item** (9,994 rows).

| Column           | Type    | Description                                      |
|------------------|---------|--------------------------------------------------|
| `sales_key`      | double  | Primary key (source `Row ID`)                    |
| `order_id`       | varchar | Degenerate dimension: the order number            |
| `order_date_key` | integer | FK → `dim_date.date_key` (order date)            |
| `ship_date_key`  | integer | FK → `dim_date.date_key` (ship date)             |
| `customer_key`   | varchar | FK → `dim_customer.customer_key`                 |
| `product_key`    | varchar | FK → `dim_product.product_key`                   |
| `geography_key`  | varchar | FK → `dim_location.geography_key`                |
| `ship_mode`      | varchar | First Class / Second Class / Standard Class / Same Day |
| `sales`          | double  | Line revenue                                     |
| `quantity`       | double  | Units ordered                                    |
| `discount`       | double  | Discount rate (0–1)                              |
| `profit`         | double  | Line profit (can be negative)                    |

### `dim_customer`

793 rows, one per customer.

| Column          | Type    | Description                                |
|-----------------|---------|--------------------------------------------|
| `customer_key`  | varchar | PK, `md5(customer_id)`                     |
| `customer_id`   | varchar | Business key                               |
| `customer_name` | varchar | Customer full name                         |
| `segment`       | varchar | Consumer / Corporate / Home Office         |

### `dim_product`

1,894 rows, one per distinct `(product_id, product_name)` pair.

| Column         | Type    | Description                                        |
|----------------|---------|----------------------------------------------------|
| `product_key`  | varchar | PK, `md5(product_id \| product_name)`              |
| `product_id`   | varchar | Business key (not unique on its own)               |
| `product_name` | varchar | Product description                                |
| `category`     | varchar | Furniture / Office Supplies / Technology           |
| `sub_category` | varchar | e.g. Chairs, Phones, Binders                       |

> **Why a composite key?** Some `product_id`s carry several names in the source. Keying on the ID alone would merge different products, or create duplicate dimension rows that fan out the fact join.

### `dim_location`

632 rows, one per distinct location.

| Column          | Type    | Description                                              |
|-----------------|---------|----------------------------------------------------------|
| `geography_key` | varchar | PK, `md5(country \| region \| state \| city \| postal_code)` |
| `country`       | varchar | Country                                                  |
| `region`        | varchar | Central / East / South / West                            |
| `state`         | varchar | State                                                    |
| `city`          | varchar | City                                                     |
| `postal_code`   | double  | Postal code                                              |

### `dim_date`

1,464 rows: every calendar day from the first order date to the last ship date.

| Column         | Type    | Description                       |
|----------------|---------|-----------------------------------|
| `date_key`     | integer | PK, `YYYYMMDD` (e.g. `20170315`)  |
| `date_day`     | date    | Calendar date                     |
| `year`         | bigint  | Year                              |
| `quarter`      | bigint  | Quarter (1–4)                     |
| `month`        | bigint  | Month number (1–12)               |
| `month_name`   | varchar | e.g. `March`                      |
| `day_of_month` | bigint  | Day (1–31)                        |
| `day_name`     | varchar | e.g. `Wednesday`                  |
| `is_weekend`   | boolean | `true` on Saturday and Sunday     |

`dim_date` is a **role-playing dimension**: `fact_orders` joins to it twice, once as order date and once as ship date.

---

## 6. Orchestration

**DAG:** [`airflow/dags/superstore_dwh_dag.py`](../airflow/dags/superstore_dwh_dag.py)

| Setting        | Value                                    |
|----------------|------------------------------------------|
| `dag_id`       | `superstore_dwh`                         |
| `schedule`     | `@daily`                                 |
| `start_date`   | 2026-10-01 UTC                           |
| `catchup`      | `False`                                  |
| `retries`      | 1, with a 2-minute delay                 |
| Paused on create | Yes (`DAGS_ARE_PAUSED_AT_CREATION`)    |

**Task flow** (strictly sequential):

```
dbt_debug → load_to_ods → dbt_test_sources → dbt_run → dbt_test
```

| # | Task               | Command (simplified)                                   | Fails when                         |
|---|--------------------|--------------------------------------------------------|------------------------------------|
| 1 | `dbt_debug`        | `dbt debug`                                            | Profile, project or DB is unreachable |
| 2 | `load_to_ods`      | `python scripts/load_to_ods.py`                        | Excel file is missing or DB is locked |
| 3 | `dbt_test_sources` | `dbt test --select source:ods --indirect-selection cautious` | Raw source tests fail          |
| 4 | `dbt_run`          | `dbt run`                                              | A model has a SQL error            |
| 5 | `dbt_test`         | `dbt test`                                             | Any data quality test fails        |

Every dbt command runs from `/opt/airflow/project/my_project` with `--profiles-dir . --target dev`, so the project uses its own committed `profiles.yml`.

`--indirect-selection cautious` on task 3 keeps the source check from also running tests on models that haven't been built yet.

---

## 7. Data Quality & Testing

**20 tests in total**, run by the `dbt_test` task.

| Model            | Column                                   | Tests                          |
|------------------|------------------------------------------|--------------------------------|
| `stg_superstore` | `row_id`                                 | `unique`, `not_null`           |
| `stg_superstore` | `order_id`, `order_date`, `customer_id`, `product_id`, `sales`, `quantity` | `not_null` |
| `fact_orders`    | `sales_key`                              | `unique`, `not_null`           |
| `fact_orders`    | `customer_key`, `product_key`, `geography_key`, `order_date_key`, `ship_date_key` | `not_null`, `relationships` |

Together, the `relationships` and `not_null` tests on the fact table guarantee that **every fact row joins to exactly one member of each dimension**. This catches fan-out or orphaned rows caused by key logic changes.

Run the tests manually:

```bash
cd my_project
dbt test --profiles-dir . --target dev                       # all tests
dbt test --profiles-dir . --target dev --select fact_orders  # one model
```

---

## 8. Infrastructure

### Container

| Component     | Detail                                                           |
|---------------|------------------------------------------------------------------|
| Base image    | `apache/airflow:3.1.0-python3.11`                                |
| Airflow mode  | `standalone` (API server, scheduler, DAG processor and triggerer in one process) |
| dbt runtime   | Separate virtualenv at `/opt/dbt_venv`                           |
| DuckDB extension | `excel`, pre-installed at build time                          |
| Port          | `8080` → Airflow UI                                              |

### Volume mounts

| Host path        | Container path            | Purpose                          |
|------------------|---------------------------|----------------------------------|
| `airflow/dags`   | `/opt/airflow/dags`       | DAG files (picked up live)       |
| `airflow/logs`   | `/opt/airflow/logs`       | Task logs, readable on the host  |
| `.` (repo root)  | `/opt/airflow/project`    | dbt project, scripts and data    |

Because the repo root is mounted, changes to dbt models, scripts or the DAG take effect on the next run **without rebuilding the image**. Only changes to the `Dockerfile` or `requirements-dbt.txt` need `docker compose up -d --build`.

### Why a separate dbt virtualenv?

Airflow pins a large dependency tree. Installing dbt into the same environment risks version conflicts (e.g. `protobuf`, `jinja2`). A dedicated venv, called by its absolute path (`/opt/dbt_venv/bin/dbt`), keeps the two isolated so each can be upgraded independently.

---

## 9. Configuration Reference

### `my_project/dbt_project.yml`

```yaml
models:
  my_project:
    staging:
      +schema: stg
      +materialized: view
    marts:
      +schema: dwh
      +materialized: table
```

dbt's default `generate_schema_name` joins the target schema (`main`) and the custom schema, which gives `main_stg` and `main_dwh`.

### `my_project/profiles.yml`

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: dev.duckdb   # relative to my_project/
      threads: 1
```

`threads: 1` is deliberate, because DuckDB allows a single writer.

### Airflow environment (`docker-compose.yml`)

| Variable                                  | Value   | Effect                        |
|-------------------------------------------|---------|-------------------------------|
| `AIRFLOW__CORE__LOAD_EXAMPLES`            | `False` | Hides the example DAGs        |
| `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS` | `True` | No login (local dev only) |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `True` | New DAGs start paused       |
| `DO_NOT_TRACK`                            | `1`     | Turns off dbt anonymous usage stats |

---

## 10. Operations Runbook

| Task                          | Command / action                                                      |
|-------------------------------|-----------------------------------------------------------------------|
| Start the stack               | `cd airflow && docker compose up -d --build`                          |
| Stop the stack                | `cd airflow && docker compose down`                                   |
| Trigger a run                 | Airflow UI → `superstore_dwh` → **Trigger**                           |
| Re-run from a failed task     | Airflow UI → failed task → **Clear** (downstream included)            |
| Rebuild the warehouse from scratch | Delete `my_project/dev.duckdb`, then trigger the DAG             |
| Run dbt in the container      | `docker exec -it superstore-airflow bash -c "cd /opt/airflow/project/my_project && /opt/dbt_venv/bin/dbt build --profiles-dir . --target dev"` |
| View task logs on the host    | `airflow/logs/dag_id=superstore_dwh/…`                               |
| Update the source data        | Replace `data/raw/Superstore.xlsx` (same sheet name), then trigger the DAG |

---

## 11. Troubleshooting

| Symptom (in task log)                                           | Cause                                                        | Fix                                                                 |
|-----------------------------------------------------------------|--------------------------------------------------------------|---------------------------------------------------------------------|
| `cd: /opt/airflow/project/...: No such file or directory`       | `DBT_DIR` in the DAG doesn't match the dbt folder name       | Set `DBT_DIR` to `{PROJECT_DIR}/my_project`                         |
| `Could not find profile named 'my_project'`                     | Profile name in `profiles.yml` doesn't match `dbt_project.yml` (case-sensitive) | Make both exactly `my_project`               |
| `mapping values are not allowed in this context`                | YAML indentation error. dbt strips leading whitespace from the file, so a fully indented file breaks | Start top-level keys (`version`, `models`) at column 0 |
| `Configuration paths exist ... which do not apply to any resources` | The key under `models:` isn't the project `name`         | Use `models: my_project:`                                           |
| `IO Error: Could not set lock on file "dev.duckdb"`             | Another process (DBeaver, CLI, notebook) has the DB open     | Close that connection, then clear the task                          |
| `Nothing to do` in `dbt_test_sources`                           | No tests are defined on the source                           | Expected until source tests are added (see Limitations)             |
| DAG doesn't appear in the UI                                    | Python error in the DAG file                                 | Check `airflow/logs/dag_processor/…`, or the import errors shown in the UI |

---

## 12. Known Limitations

| Limitation                           | Impact                                              | Possible improvement                                     |
|--------------------------------------|-----------------------------------------------------|----------------------------------------------------------|
| Full refresh on every run            | Fine at 10 K rows, but doesn't scale                | Incremental `fact_orders` (`materialized='incremental'`) |
| No tests on the raw source           | `dbt_test_sources` currently does nothing           | Add `not_null` / `unique` on `"Row ID"` in `sources.yml` |
| `Row ID` / `Postal Code` typed as `double` | Inherited from Excel inference                | Cast to `integer` / `varchar` in staging                 |
| Customer attributes use `max()`      | No history of changes to segment or name            | SCD Type 2 with dbt snapshots                            |
| Schemas named `main_stg` / `main_dwh` | Prefix comes from dbt's default naming             | Custom `generate_schema_name` macro                      |
| Local, single-container setup        | No auth, no high availability                       | CeleryExecutor or Kubernetes, with a managed warehouse   |
| Deprecated generic test syntax       | dbt shows warnings (5 occurrences)                  | Move test arguments under `arguments:`                   |
