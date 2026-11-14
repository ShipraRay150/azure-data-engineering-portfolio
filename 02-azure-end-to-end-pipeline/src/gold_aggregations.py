"""Build Gold business aggregates from curated Silver Delta data."""
from pyspark.sql import functions as F
from config.project_config import SILVER_PATH, GOLD_PATH, CATALOG, SCHEMA


def run(spark):
    silver = spark.read.format("delta").load(SILVER_PATH)

    daily_sales = (
        silver.groupBy("order_date", "country")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("gross_amount").alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.avg("gross_amount").alias("average_order_value"),
        )
    )
    product_performance = (
        silver.groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("gross_amount").alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
    )

    daily_path = GOLD_PATH + "daily_sales/"
    product_path = GOLD_PATH + "product_performance/"
    daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(daily_path)
    product_performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(product_path)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_daily_sales "
        f"USING DELTA LOCATION '{daily_path}'"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold_product_performance "
        f"USING DELTA LOCATION '{product_path}'"
    )


if __name__ == "__main__":
    run(spark)  # noqa: F821 - provided by the Databricks Python runtime
