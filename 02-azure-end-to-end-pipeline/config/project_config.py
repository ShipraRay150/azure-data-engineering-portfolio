"""Central configuration for the Azure retail lakehouse project.

Set these values through Databricks job parameters or environment variables.
Never store credentials or access keys in this file.
"""
import os

STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "<storage-account>")
LANDING_PATH = os.getenv(
    "LANDING_PATH",
    f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net/retail/orders/",
)
BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/retail/orders/",
)
SILVER_PATH = os.getenv(
    "SILVER_PATH",
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/retail/orders/",
)
GOLD_PATH = os.getenv(
    "GOLD_PATH",
    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/retail/",
)
QUARANTINE_PATH = os.getenv(
    "QUARANTINE_PATH",
    f"abfss://quarantine@{STORAGE_ACCOUNT}.dfs.core.windows.net/retail/orders/",
)
CHECKPOINT_ROOT = os.getenv(
    "CHECKPOINT_ROOT",
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/retail/",
)
SCHEMA_ROOT = os.getenv(
    "SCHEMA_ROOT",
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_schemas/retail/orders/",
)
CATALOG = os.getenv("DATABRICKS_CATALOG", "retail_lakehouse")
SCHEMA = os.getenv("DATABRICKS_SCHEMA", "retail")
