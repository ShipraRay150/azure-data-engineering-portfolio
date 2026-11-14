# 02 - End-to-End Azure Retail Data Engineering Pipeline

A portfolio-ready Azure Data Engineering project using synthetic retail orders. It demonstrates an end-to-end lakehouse pipeline with ADLS Gen2, Azure Data Factory (ADF), Azure Databricks, PySpark, Auto Loader, Delta Lake, Databricks Workflows and Databricks SQL.

> **Status:** Implementation blueprint and runnable Databricks source templates. Azure resources must be provisioned and configured in your own subscription before cloud execution. No production deployment is claimed.

## Architecture

```text
Synthetic Retail CSV files
        |
        v
ADLS Gen2 - landing container
        |
        v
Azure Data Factory (schedule / trigger)
        |
        v
Azure Databricks Workflow
        |
        +--> Bronze: Auto Loader -> Delta (append-only raw records)
        |
        +--> Silver: type casting, standardization, deduplication,
        |            null checks, invalid-record quarantine
        |
        +--> Gold: daily sales, product performance, customer metrics
        |
        v
Databricks SQL dashboards / analytics
```

## Technology stack

- Azure Data Lake Storage Gen2: landing, bronze, silver, gold and quarantine storage
- Azure Data Factory: trigger and orchestration entry point
- Azure Databricks + PySpark: distributed transformations
- Auto Loader (cloudFiles): incremental file discovery
- Delta Lake: reliable ACID tables
- Databricks Workflows: task dependencies and retries
- Databricks SQL: analytics queries
- Python / pytest: local data generation and unit tests
- GitHub: version control and project documentation

## Repository structure

```text
02-azure-end-to-end-pipeline/
├── README.md
├── config/
│   └── project_config.py
├── data/
│   └── generate_retail_data.py
├── src/
│   ├── bronze_ingestion.py
│   ├── silver_transform.py
│   ├── gold_aggregations.py
│   └── data_quality.py
├── sql/
│   └── analytics.sql
├── tests/
│   └── test_data_quality.py
└── workflows/
    └── databricks_workflow.example.json
```

## 1. Generate synthetic source data

Run locally with Python 3.10+:

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install pandas pytest
python 02-azure-end-to-end-pipeline/data/generate_retail_data.py
```

The generator writes synthetic order records to `data/generated/orders/`. Upload the CSV files to the ADLS Gen2 landing path, for example:

```text
abfss://landing@<storage-account>.dfs.core.windows.net/retail/orders/
```

## 2. Configure Databricks

1. Create an ADLS Gen2 storage account with hierarchical namespace enabled.
2. Create containers or directories for landing, bronze, silver, gold and quarantine.
3. Configure secure access using a Unity Catalog storage credential / external location or a service principal backed by a secret scope. Prefer managed identity where supported by your workspace setup.
4. Create a Databricks cluster or serverless environment with compatible PySpark and Delta Lake support.
5. Set the project configuration values in the Databricks job parameters or environment; do not commit credentials.
6. Import the Python source files into a Databricks Git folder or workspace and configure the job.

Required configuration values are documented in `config/project_config.py`.

## 3. Execute the lakehouse layers

Run the scripts in this order from a Databricks Workflow:

1. `src/bronze_ingestion.py` - Auto Loader discovers new CSV files and writes a Delta Bronze table.
2. `src/silver_transform.py` - standardizes fields, validates records, removes duplicates and writes Silver plus quarantine Delta tables.
3. `src/gold_aggregations.py` - creates daily sales and product performance Gold tables.

The scripts use `trigger(availableNow=True)` so each run processes newly discovered files and then stops. Checkpoint locations must be durable and unique per stream.

## 4. Azure Data Factory orchestration

Create an ADF pipeline with a Databricks Notebook activity or Databricks Job activity. Pass the source and target paths as parameters. Schedule or event-trigger the ADF pipeline based on your ingestion requirement. Keep credentials in Key Vault / managed identity; never place secrets in pipeline JSON or GitHub.

## 5. Databricks Workflow

The example job definition in `workflows/databricks_workflow.example.json` shows the task dependency order. Adapt the cluster / environment configuration, source paths, and workspace-specific task settings before importing it.

## 6. Analytics

Run `sql/analytics.sql` against the Gold tables to answer:
- What is daily revenue and order volume?
- Which products generate the most revenue?
- What is the average order value?
- How do sales change over time?

## 7. Quality and testing

The project includes reusable data-quality rules and local unit tests. Run:

```bash
pip install pytest
pytest 02-azure-end-to-end-pipeline/tests -q
```

In Databricks, also validate row counts, null rates, duplicate order IDs, rejected records and job run status after each execution.

## Security and engineering practices

- Use synthetic data only in this public repository.
- Never commit secrets, access keys, SAS tokens, customer information or employer-owned code.
- Use least-privilege access and managed identity / secret scopes.
- Keep Auto Loader checkpoints and schema state in durable storage.
- Make transformations idempotent and monitor failed / quarantined records.
- Apply partitioning only after measuring data volume and query patterns.

## Next implementation milestones

- [ ] Provision ADLS Gen2 and secure access
- [ ] Upload generated CSV files to landing
- [ ] Run Auto Loader Bronze ingestion in Databricks
- [ ] Run Silver cleansing and quarantine
- [ ] Build Gold aggregates and validate SQL
- [ ] Configure Databricks Workflow
- [ ] Trigger the Databricks job from ADF
- [ ] Add screenshots of successful runs and analytics (without exposing secrets)

## Learning outcomes

This project demonstrates practical understanding of cloud storage, incremental ingestion, Spark transformations, Delta Lake medallion design, orchestration, data quality, analytics and secure deployment patterns.
