# Enterprise Retail Sales ETL Pipeline – Azure Data Factory & Databricks

## Overview

An end-to-end **Enterprise Retail ETL Pipeline** that ingests daily sales data from multiple source systems (POS terminals, e-commerce API, ERP flat files), transforms and consolidates it using Azure Databricks, and loads it into a Synapse Analytics data warehouse for BI reporting.

Implements **Slowly Changing Dimensions (SCD Type 2)** for customer and product master data, and incremental load patterns for fact tables.

---

## Architecture

```
POS System (CSV)   E-Commerce API (JSON)   ERP System (Parquet)
        │                   │                       │
        └───────────────────┴───────────────────────┘
                            │
                    Azure Data Factory
                    (Orchestration + Copy Activity)
                            │
                    ADLS Gen2 – Bronze
                            │
                    Databricks (PySpark)
                    ├── Silver: Cleanse + Conform
                    └── Gold: Star Schema (Fact + Dim)
                            │
                    Azure Synapse Analytics
                            │
                    Power BI Dashboard
```

---

## Tech Stack

| Component | Tool |
|---|---|
| Orchestration | Azure Data Factory (ADF) |
| Storage | ADLS Gen2 |
| Processing | Azure Databricks + PySpark |
| Warehouse | Azure Synapse Analytics |
| Format | Delta Lake (Bronze/Silver/Gold) |
| SCD | Delta MERGE (Type 2 pattern) |
| Language | Python 3.10, SQL |

---

## Project Structure

```
📁 data/              → Sample POS CSV, e-commerce JSON, product master
📁 notebooks/         → ETL notebooks (ingest, transform, SCD, fact load)
📁 sql/               → Synapse DDL, stored procs, BI views
📄 README.md
📄 requirements.txt
```

---

## Data Model (Star Schema)

### Fact Table
- `fact_sales` – transaction-level grain: order_id, date_key, customer_key, product_key, quantity, unit_price, discount, net_revenue

### Dimension Tables
- `dim_customer` – SCD Type 2 (tracks address/segment changes over time)
- `dim_product`  – SCD Type 2 (tracks price/category changes)
- `dim_date`     – Pre-built calendar dimension (2020–2030)
- `dim_store`    – Store master with region hierarchy

---

## Pipeline Stages

### Stage 1 – ADF Ingest
- Parameterized Copy Activities pull source files by `run_date`
- Schema mapping applied at ADF level
- Files land in Bronze with `_source` and `_landed_at` metadata

### Stage 2 – Silver Transform (Databricks)
- Standardize column names across 3 source systems
- Validate: price > 0, quantity > 0, valid product_id
- Deduplicate on `order_id`

### Stage 3 – Gold: SCD Type 2 Dimensions
- Delta MERGE detects changed rows → closes old record, inserts new
- `is_current`, `effective_date`, `end_date` flags maintained

### Stage 4 – Gold: Fact Load (Incremental)
- Append-only insert for new `order_id` values
- Lookup keys from dimension tables for surrogate key assignment

---

## Key Learnings

- SCD Type 2 implementation using Delta MERGE without CDC tools
- Cross-source schema harmonization (3 different naming conventions)
- ADF parameterized pipelines for daily scheduling
- Slowly changing dimension audit trail — full history preserved
