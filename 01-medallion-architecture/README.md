# Project 01: Retail Medallion Architecture

## Business scenario

A retailer receives daily order, customer and product CSV files. The pipeline ingests the files, preserves source data, validates and enriches orders, and produces analytics-ready sales tables.

## Architecture

```text
Synthetic CSV files
   |-- orders.csv
   |-- customers.csv
   |-- products.csv
          |
          v
   BRONZE (Delta)
   Raw source records + ingestion metadata
          |
          v
   SILVER (Delta)
   Type casting, validation, deduplication,
   rejected-record quarantine, enrichment
          |
          v
   GOLD (Delta)
   Daily sales, product sales, customer sales
```

## Technology

- Python 3.10 or 3.11
- PySpark 3.5.x
- Delta Lake 3.3.x
- Local filesystem for this first iteration

## Prerequisites

- Python 3.10 or 3.11
- Java 11 or 17 (JAVA_HOME configured)
- Git

Check versions:

```bash
python --version
java -version
git --version
```

## Setup (Windows PowerShell)

From the repository root:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r 01-medallion-architecture/requirements.txt
```

If PowerShell blocks activation, use:

```powershell
.\.venv\Scripts\python.exe -m pip install -r 01-medallion-architecture/requirements.txt
```

## Run the pipeline

From the repository root:

```powershell
python 01-medallion-architecture/src/run_pipeline.py
```

The pipeline writes Delta tables under:

```text
01-medallion-architecture/warehouse/
  bronze/orders/
  bronze/customers/
  bronze/products/
  silver/orders/
  silver/rejected_orders/
  gold/daily_sales/
  gold/product_sales/
  gold/customer_sales/
  quality/summary/
```

## What to inspect

1. Bronze tables retain source values and add `ingested_at` and `source_file`.
2. Silver orders contain typed, validated and enriched records.
3. Invalid orders are written to `silver/rejected_orders`.
4. Gold tables contain business aggregations.
5. The quality summary reports input, accepted and rejected counts.

## Important local-vs-Azure distinction

This first version uses batch file reads because it runs on a local machine. It does **not** use Auto Loader. In the Azure phase, the ingestion component will be adapted to Databricks Auto Loader with cloudFiles, schema location and checkpointing, while retaining the downstream Bronze/Silver/Gold design.

## Next engineering enhancements

- Add deterministic business-key deduplication and late-arriving update handling.
- Implement Delta MERGE for incremental upserts.
- Add unit and integration tests.
- Add structured logging and pipeline run metadata.
- Add Databricks Workflows and Azure deployment configuration.
- Add performance experiments (partitioning, file sizing, query plans).

## Data

All CSV files are synthetic and included for learning. Do not use confidential or client data.
