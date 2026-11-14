"""Clean Bronze records, deduplicate, and write Silver + quarantine Delta tables."""
from pyspark.sql import functions as F
from config.project_config import BRONZE_PATH, SILVER_PATH, QUARANTINE_PATH, CATALOG, SCHEMA
from src.data_quality import split_valid_and_invalid


def run(spark):
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    standardized = (
        bronze
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("country", F.initcap(F.trim("country")))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
    )
    valid, invalid = split_valid_and_invalid(standardized)

    # Keep the latest ingested version per order_id.
    from pyspark.sql.window import Window
    w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
    deduplicated = (
        valid.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    (deduplicated.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(SILVER_PATH))
    (invalid.write.format("delta").mode("append").save(QUARANTINE_PATH))

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.silver_orders "
        f"USING DELTA LOCATION '{SILVER_PATH}'"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.quarantined_orders "
        f"USING DELTA LOCATION '{QUARANTINE_PATH}'"
    )


if __name__ == "__main__":
    run(spark)  # noqa: F821 - provided by the Databricks Python runtime
