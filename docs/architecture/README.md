# Architecture documentation

Add your own architecture diagram here as the project evolves.

Current flow:
CSV source files -> Bronze Delta -> Silver validation/enrichment -> Gold aggregates.

Local implementation uses batch reads. Azure implementation will add ADLS Gen2, Auto Loader, Unity Catalog and Databricks Workflows.
