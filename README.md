# Azure Data Engineering Portfolio

**Portfolio focus:** Azure Data Engineering | Senior Data Engineering | PySpark | Delta Lake | SQL

This repository contains hands-on, reproducible data engineering projects. The first project is a local Medallion Architecture pipeline that can later be adapted to Azure Databricks and ADLS Gen2.

## Projects

| Project | Description | Status |
|---|---|---|
| [01 - Retail Medallion Architecture](01-medallion-architecture/README.md) | Batch ingestion and Bronze/Silver/Gold Delta tables using PySpark | In progress |
| [02 - End-to-End Azure Retail Pipeline](02-azure-end-to-end-pipeline/README.md) | ADLS Gen2, ADF, Databricks Auto Loader, Delta Lake and Gold analytics | Implementation templates added |
| 02 - Incremental ETL Framework | Planned: incremental processing, merge/upsert patterns and replay | Planned |
| 03 - Data Quality Framework | Planned: reusable validation, quarantine and monitoring | Planned |

## Technology roadmap

- Python, PySpark and SQL
- Delta Lake and Medallion Architecture
- Git and GitHub
- Azure Databricks, ADLS Gen2 and Auto Loader (Azure implementation phase)
- Databricks Workflows, testing, monitoring and optimization

## Run locally

See the [Project 01 setup guide](01-medallion-architecture/README.md).

## Engineering principles

- Keep raw source data separate from curated data.
- Make transformations repeatable and observable.
- Validate records and quarantine invalid data.
- Document assumptions and operational behavior.
- Never commit credentials, customer data or employer-owned code.

> This is a learning portfolio using synthetic data. It does not represent a production deployment or client implementation.
