# Superstore Data Warehouse — Airflow · dbt · DuckDB

An end-to-end batch ELT pipeline that turns the raw **Superstore** sales spreadsheet into a tested **star-schema data warehouse**. Apache Airflow orchestrates the run, dbt handles transformation and data quality, and DuckDB serves as the embedded analytical database. Everything runs locally in a single Docker container.

![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt--core-1.12-FF694B?logo=dbt&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5-FFF000?logo=duckdb&logoColor=black)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

---

## Table of Contents

- [Architecture](#architecture)
- [Data Model](#data-model)
- [Pipeline (Airflow DAG)](#pipeline-airflow-dag)
- [Data Quality](#data-quality)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Querying the Warehouse](#querying-the-warehouse)
- [Design Decisions](#design-decisions)
- [Roadmap](#roadmap)

---

## Architecture

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Superstore.xlsx │ ──▶ │   ODS layer     │ ──▶ │  Staging layer   │ ──▶ │   DWH (marts)    │
│   data/raw/      │     │ ods.superstore  │     │ main_stg (views) │     │ main_dwh (tables)│
└──────────────────┘     └─────────────────┘     └──────────────────┘     └──────────────────┘
        Extract & Load (Python + DuckDB)          Transform & Test (dbt)

                         ▲ orchestrated end-to-end by Apache Airflow ▲
```

| Layer       | Schema     | Materialization | Purpose                                                        |
|-------------|------------|-----------------|----------------------------------------------------------------|
| **ODS**     | `ods`      | table           | 1:1 raw copy of the Excel sheet, reloaded on every run          |
| **Staging** | `main_stg` | view            | Renamed to `snake_case`, typed (dates cast), one row per order line |
| **Marts**   | `main_dwh` | table           | Kimball star schema: one fact table and four dimensions         |

---

## Data Model

A star schema at the grain of **one row per order line item**.

```
                    ┌───────────────┐
                    │ dim_customer  │
                    │ customer_key  │
                    └───────┬───────┘
                            │
┌───────────────┐   ┌───────┴───────┐   ┌────────────────┐
│  dim_product  │───│  fact_orders  │───│  dim_location  │
│  product_key  │   │  sales_key    │   │  geography_key │
└───────────────┘   └───────┬───────┘   └────────────────┘
                            │  order_date_key / ship_date_key
                    ┌───────┴───────┐
                    │   dim_date    │   (role-playing)
                    │   date_key    │
                    └───────────────┘
```

| Model          | Rows   | Key             | Description                                                         |
|----------------|--------|-----------------|---------------------------------------------------------------------|
| `fact_orders`  | 9,994  | `sales_key`     | Sales, quantity, discount, profit, ship mode, plus FKs to every dimension |
| `dim_customer` | 793    | `customer_key`  | Customer name and segment                                           |
| `dim_product`  | 1,894  | `product_key`   | Product name, category, sub-category                                |
| `dim_location` | 632    | `geography_key` | Country, region, state, city, postal code                           |
| `dim_date`     | 1,464  | `date_key`      | Calendar spine (`YYYYMMDD`) with year, quarter, month, weekday, weekend flag |

---

## Pipeline (Airflow DAG)

DAG `superstore_dwh` runs `@daily` (no catchup, retries once after 2 minutes):

```
dbt_debug ──▶ load_to_ods ──▶ dbt_test_sources ──▶ dbt_run ──▶ dbt_test
```

| Task               | What it does                                                               |
|--------------------|----------------------------------------------------------------------------|
| `dbt_debug`        | Checks the dbt installation, profile and database connection              |
| `load_to_ods`      | Runs [`scripts/load_to_ods.py`](scripts/load_to_ods.py) to load the Excel sheet into `ods.superstore` with DuckDB's `excel` extension |
| `dbt_test_sources` | Tests the raw source before any model is built                             |
| `dbt_run`          | Builds the staging views and mart tables                                   |
| `dbt_test`         | Runs every model test. A failure here stops the run and marks it failed   |

dbt is installed in its own virtualenv (`/opt/dbt_venv`) inside the Airflow image, so its dependencies never conflict with Airflow's.

---

## Data Quality

The project defines **20 dbt tests**:

- **Staging:** `row_id` is `unique` and `not_null`. Every business-critical column (order, customer, product, dates, sales, quantity) is `not_null`.
- **Fact table:** `sales_key` is `unique` and `not_null`. All five foreign keys are `not_null` and have a `relationships` test against their dimension, which guarantees referential integrity across the star schema.

---

## Project Structure

```
.
├── airflow/
│   ├── dags/
│   │   └── superstore_dwh_dag.py    # Airflow DAG definition
│   ├── Dockerfile                   # Airflow 3.1 image + isolated dbt virtualenv
│   ├── docker-compose.yml           # Single-container local deployment
│   └── requirements-dbt.txt         # Pinned dbt-core / dbt-duckdb / duckdb
├── data/
│   └── raw/
│       └── Superstore.xlsx          # Source dataset
├── scripts/
│   └── load_to_ods.py               # Extract & Load into the ODS schema
└── my_project/                      # dbt project
    ├── dbt_project.yml
    ├── profiles.yml                 # DuckDB target (dev.duckdb)
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   ├── schema.yml
        │   └── stg_superstore.sql
        └── marts/
            ├── schema.yml
            ├── fact_orders.sql
            ├── dim_customer.sql
            ├── dim_product.sql
            ├── dim_location.sql
            └── dim_date.sql
```

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose
- About 4 GB of RAM available to Docker

### 1. Clone the repository

```bash
git clone https://github.com/MinaMiladd/superstore_DBT_Project.git
cd superstore_DBT_Project
```

### 2. Build and start Airflow

```bash
cd airflow
docker compose up -d --build
```

The first build installs dbt and the DuckDB Excel extension, which takes a few minutes.

### 3. Open the Airflow UI

Go to **http://localhost:8080**. The local setup has authentication turned off, so no login is needed.

### 4. Run the pipeline

1. Find the **`superstore_dwh`** DAG. It is paused when first created.
2. Unpause it, then click **Trigger**.
3. Follow the run in the **Graph** view. All five tasks should turn green.

### 5. Stop the stack

```bash
docker compose down
```

### Running dbt without Airflow (optional)

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r airflow/requirements-dbt.txt

python scripts/load_to_ods.py
cd my_project
dbt run  --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
```

---

## Querying the Warehouse

The warehouse is one file, `my_project/dev.duckdb`, which the pipeline creates. Open it with the DuckDB CLI, DBeaver or Python:

```sql
-- Profit by category and year
SELECT
    p.category,
    d.year,
    ROUND(SUM(f.sales), 2)  AS total_sales,
    ROUND(SUM(f.profit), 2) AS total_profit
FROM main_dwh.fact_orders  f
JOIN main_dwh.dim_product  p ON f.product_key    = p.product_key
JOIN main_dwh.dim_date     d ON f.order_date_key = d.date_key
GROUP BY p.category, d.year
ORDER BY d.year, total_profit DESC;
```

> **Note:** DuckDB allows only one writer at a time. Close any open connection to `dev.duckdb` before triggering the DAG, or the run will fail with a lock error.

---

## Design Decisions

- **Hash surrogate keys (`md5`).** The keys are deterministic, so a fact row maps to the same dimension key on every rebuild, without sequences or lookups.
- **Composite product key.** In the source data, some `Product ID`s map to more than one product name. `product_key` hashes both columns so that no product is lost or double-counted.
- **Generated date spine.** `dim_date` covers every day from the earliest order date to the latest ship date, gaps included. It is used twice by the fact table, as order date and as ship date.
- **NULL-safe joins.** The location join uses `IS NOT DISTINCT FROM` on `postal_code`, so rows with a missing postal code still match their dimension.
- **Isolated dbt runtime.** Running dbt from a separate virtualenv keeps Airflow's dependency set untouched and makes either tool easy to upgrade.

---

## Roadmap

- [ ] Add source-level tests (freshness, `not_null` on raw columns)
- [ ] Incremental loading for `fact_orders`
- [ ] Generate and host dbt docs (`dbt docs generate`)
- [ ] CI workflow running `dbt build` on pull requests
- [ ] Custom `generate_schema_name` macro for clean `stg` / `dwh` schema names

---

## Author

**Mina Milad Matta**, DataOps Engineering, NTI
GitHub: [@MinaMiladd](https://github.com/MinaMiladd)
