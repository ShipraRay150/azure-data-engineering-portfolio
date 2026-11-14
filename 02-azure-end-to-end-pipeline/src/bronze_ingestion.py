"""Incremental CSV ingestion with Databricks Auto Loader into Delta Bronze."""
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from config.project_config import (
    LANDING_PATH, BRONZE_PATH, CHECKPOINT_ROOT, SCHEMA_ROOT, CATALOG, SCHEMA
)

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
])


def run(spark):
    (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_ROOT)
        .option("header", "true")
        .schema(order_schema)
        .load(LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_ROOT + "bronze_orders/")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
        .awaitTermination())

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.bronze_orders "
        f"USING DELTA LOCATION '{BRONZE_PATH}'"
    )


if __name__ == "__main__":
    run(spark)  # noqa: F821 - provided by the Databricks Python runtime
