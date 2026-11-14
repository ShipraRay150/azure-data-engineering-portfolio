"""Reusable PySpark data-quality checks."""
from pyspark.sql import functions as F


def split_valid_and_invalid(df):
    """Return (valid, invalid); invalid records include a human-readable reason."""
    checked = df.withColumn(
        "_quality_error",
        F.when(F.col("order_id").isNull(), F.lit("missing_order_id"))
         .when(F.col("customer_id").isNull(), F.lit("missing_customer_id"))
         .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity"))
         .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid_unit_price"))
         .when(F.col("order_timestamp").isNull(), F.lit("missing_order_timestamp"))
    )
    return (
        checked.filter(F.col("_quality_error").isNull()).drop("_quality_error"),
        checked.filter(F.col("_quality_error").isNotNull()),
    )


def quality_summary(df):
    """Return basic counts suitable for logging / monitoring."""
    return df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0)).alias("null_order_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
