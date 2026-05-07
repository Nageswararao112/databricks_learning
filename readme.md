# AdventureWorks Medallion Pipeline — Azure Databricks

A production-ready end-to-end data pipeline built on Azure Databricks implementing the **Bronze → Silver → Gold** medallion architecture using the AdventureWorks dataset stored in ADLS Gen2.

---

## Architecture Overview

```
ADLS Gen2 (source)
    └── Returns/
        ├── AdventureWorks_Calendar.csv
        ├── AdventureWorks_Customers.csv
        ├── AdventureWorks_Products.csv
        ├── AdventureWorks_Product_Categories.csv
        ├── AdventureWorks_Product_Subcategories.csv
        ├── AdventureWorks_Territories.csv
        ├── AdventureWorks_Sales_2015.csv
        ├── AdventureWorks_Sales_2016.csv
        └── AdventureWorks_Sales_2017.csv
            │
            ▼
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
    │  Raw Delta  │     │ Cleansed DQ │     │ Star Schema │
    └─────────────┘     └─────────────┘     └─────────────┘
            │
    demo.pipeline.batch_control  (orchestration control table)
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Storage | ADLS Gen2 (`awprojectadls0506`) |
| Compute | Azure Databricks (Serverless) |
| Table format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows (Jobs & Pipelines) |
| Identity | Azure Managed Identity + Unity Catalog External Location |
| Language | PySpark (Python) |

---

## Project Structure

```
/Repos/awproject/databricks_learning/
│
├── config/
│   ├── 00_config            # Central config — env detection, paths, schema creation
│   └── 00_batch_control     # Batch control table — logging, watermark, restart logic
│
├── bronze/
│   └── 01_bronze            # Raw ingestion — CSV → Delta, no transforms
│
├── silver/
│   └── 02_silver            # Cleansing — type cast, rename, DQ checks
│
├── gold/
│   └── 03_gold              # Star schema — dims (SCD1/SCD2) + fact table
│
└── orchestration/
    └── 04_orchestration     # Master pipeline runner — bronze → silver → gold
```

---

## Medallion Layers

### Bronze — Raw Ingestion
- Reads all 9 CSV files from ADLS Gen2 as raw strings (`inferSchema=false`)
- No type casting, no renaming — data lands exactly as received
- Adds 4 audit columns to every row: `source_file`, `ingestion_timestamp`, `ingestion_date`, `batch_id`
- Writes as Delta tables with full overwrite (`mode=overwrite`)
- All tables are `FULL` load — file sources do not use watermarks

### Silver — Cleansing & DQ
- Reads from Bronze Delta tables
- Applies type casting (dates → `M/d/yyyy`, strings → int/double)
- Renames all columns to `snake_case`
- Runs DQ checks per table:
  - Null checks on required columns
  - Duplicate checks on primary keys
  - Validity checks (e.g. `product_cost <= product_price`, `order_quantity > 0`)
- All rows written to Silver — failed rows tagged with `dq_status = FAIL` and `dq_failed_reason`
- No rows are removed — quarantine is in-table via status columns
- Bronze audit columns replaced with Silver audit columns

### Gold — Star Schema
- Reads only `dq_status = PASS` rows from Silver
- Implements a **snowflake schema**:

```
fact_sales
  ├── dim_customer      (SCD Type 2)
  ├── dim_product       (SCD Type 2) ──▶ dim_product_category (SCD Type 1)
  ├── dim_territory     (SCD Type 1)
  └── dim_date          (static — generated 2015–2018)
```

- Surrogate keys generated for all dimensions
- SCD Type 2 dims track history with `effective_date`, `expiry_date`, `is_current`
- `fact_sales` resolves all surrogate keys from current dimension rows
- `date_key` derived as `yyyyMMdd` integer from `order_date`

---

## Batch Control Table

All pipeline runs are tracked in a single Delta table: `demo.pipeline.batch_control`

| Column | Purpose |
|---|---|
| `id` | Auto-increment identity — unique per row |
| `table_name` | Logical table name |
| `layer` | `bronze` / `silver` / `gold` |
| `load_type` | `FULL` / `INCR` |
| `source_type` | `file` / `delta` / `db` |
| `watermark_col` | Column used for incremental filtering |
| `last_load_timestamp` | Max watermark from last successful run |
| `batch_start_time` | When the run started |
| `batch_end_time` | When the run completed |
| `status` | `RUNNING` / `SUCCESS` / `FAILED` |
| `rows_loaded` | Row count written in this run |

### Restart Logic
- On rerun, each table checks its `status` in the control table
- `SUCCESS` → skip (already done)
- `FAILED` or `RUNNING` → rerun
- `NULL` → first ever run

### Reset Control Table
```sql
TRUNCATE TABLE demo.pipeline.batch_control;
-- Then rerun 00_batch_control to re-seed
```

---

## Configuration

All environment config lives in `00_config`. Environment is auto-detected from the workspace URL:

```python
# dev  → awproject-dev.azuredatabricks.net
# qa   → awproject-qa.azuredatabricks.net
# prod → awproject-prod.azuredatabricks.net
```

| Variable | dev | qa | prod |
|---|---|---|---|
| `ADLS_ACCOUNT` | `awprojectadls0506` | `awprojectadlsqa` | `awprojectadlsprod` |
| `CATALOG` | `demo` | `demo_qa` | `demo_prod` |
| `BRONZE` | `demo.bronze` | `demo_qa.bronze` | `demo_prod.bronze` |
| `SILVER` | `demo.silver` | `demo_qa.silver` | `demo_prod.silver` |
| `GOLD` | `demo.gold` | `demo_qa.gold` | `demo_prod.gold` |

---

## ADLS Gen2 Connection

Authentication via **Unity Catalog External Location** + **Azure Managed Identity**:

1. `unity-catalog-access-connector` assigned `Storage Blob Data Contributor` on `awprojectadls0506`
2. Storage credential created in Unity Catalog
3. External location: `abfss://source@awprojectadls0506.dfs.core.windows.net/Returns`
4. Notebooks read via `spark.read` — no `spark.conf.set()` needed

---

## Orchestration

Pipeline runs daily via **Databricks Workflows** (Jobs & Pipelines):

```
Task 1: bronze  ──▶  Task 2: silver  ──▶  Task 3: gold
```

- Each task runs the corresponding notebook
- Tasks have `depends_on` set so silver only starts after bronze succeeds
- Email notification on failure to `nageswararaobhatraju@gmail.com`
- Pre-flight check between layers verifies all tables in previous layer have `status = SUCCESS`

---

## How to Run

### Manual run (notebook by notebook)
```python
# Cell 1 in any notebook
%run /Repos/awproject/databricks_learning/config/00_config

# Cell 2
%run /Repos/awproject/databricks_learning/config/00_batch_control
```

### Full pipeline run
Trigger via **Jobs & Pipelines** → select `awproject_medallion_pipeline` → **Run now**

### Monitor pipeline status
```sql
SELECT table_name, layer, status, rows_loaded,
       batch_start_time, batch_end_time
FROM   demo.pipeline.batch_control
ORDER  BY layer, id;
```

### Check DQ results in Silver
```sql
-- Summary per table
SELECT dq_status, dq_failed_reason, COUNT(*) AS cnt
FROM   demo.silver.sales
GROUP  BY dq_status, dq_failed_reason
ORDER  BY cnt DESC;
```

---

## Gold Layer Queries

```sql
-- Total sales by territory
SELECT t.region, t.country,
       SUM(f.order_quantity) AS total_units
FROM   demo.gold.fact_sales   f
JOIN   demo.gold.dim_territory t ON f.territory_sk = t.territory_sk
GROUP  BY t.region, t.country
ORDER  BY total_units DESC;

-- Sales by product category and year
SELECT pc.category_name,
       d.year,
       SUM(f.order_quantity) AS total_units
FROM   demo.gold.fact_sales          f
JOIN   demo.gold.dim_product         p  ON f.product_sk       = p.product_sk  AND p.is_current = true
JOIN   demo.gold.dim_product_category pc ON p.product_category_sk = pc.product_category_sk
JOIN   demo.gold.dim_date            d  ON f.date_key          = d.date_key
GROUP  BY pc.category_name, d.year
ORDER  BY d.year, total_units DESC;

-- Customer purchase history (SCD2 aware)
SELECT c.full_name, c.annual_income, c.occupation,
       c.effective_date, c.expiry_date, c.is_current
FROM   demo.gold.dim_customer c
WHERE  c.customer_key = 11000
ORDER  BY c.effective_date;
```

---

## Author

**Nageswararao Bhatraju**
`nageswararaobhatraju@gmail.com`
