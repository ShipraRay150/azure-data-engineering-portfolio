from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data" / "raw"
WAREHOUSE_DIR = PROJECT_DIR / "warehouse"


def create_spark():
    builder = (
        SparkSession.builder
        .appName("RetailMedallionPipeline")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta(df, path):
    df.write.format("delta").mode("overwrite").save(str(path))


def read_delta(spark, path):
    return spark.read.format("delta").load(str(path))


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n[1/4] Reading source CSV files")
        orders_raw = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(RAW_DIR / "orders.csv"))
            .withColumn("source_file", F.input_file_name())
            .withColumn("ingested_at", F.current_timestamp())
        )
        customers_raw = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(RAW_DIR / "customers.csv"))
            .withColumn("source_file", F.input_file_name())
            .withColumn("ingested_at", F.current_timestamp())
        )
        products_raw = (
            spark.read.option("header", True).option("inferSchema", True)
            .csv(str(RAW_DIR / "products.csv"))
            .withColumn("source_file", F.input_file_name())
            .withColumn("ingested_at", F.current_timestamp())
        )

        print("[2/4] Writing Bronze Delta tables")
        bronze = WAREHOUSE_DIR / "bronze"
        write_delta(orders_raw, bronze / "orders")
        write_delta(customers_raw, bronze / "customers")
        write_delta(products_raw, bronze / "products")

        print("[3/4] Validating and transforming Silver tables")
        orders = read_delta(spark, bronze / "orders")
        customers = read_delta(spark, bronze / "customers")
        products = read_delta(spark, bronze / "products")

        typed = (
            orders
            .withColumn("order_id", F.col("order_id").cast("string"))
            .withColumn("customer_id", F.col("customer_id").cast("string"))
            .withColumn("product_id", F.col("product_id").cast("string"))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("double"))
            .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        )

        # For production, prefer a source event/update timestamp as the
        # deterministic tie-breaker. This sample uses ingestion time.
        dedupe_window = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
        deduped = (
            typed.withColumn("_row_num", F.row_number().over(dedupe_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        invalid_condition = (
            F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("product_id").isNull()
            | F.col("order_date").isNull()
            | F.col("quantity").isNull()
            | (F.col("quantity") <= 0)
            | F.col("unit_price").isNull()
            | (F.col("unit_price") < 0)
        )

        rejected = deduped.filter(invalid_condition).withColumn(
            "rejection_reason",
            F.concat_ws(
                "; ",
                F.when(F.col("order_id").isNull(), F.lit("missing order_id")),
                F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
                F.when(F.col("product_id").isNull(), F.lit("missing product_id")),
                F.when(F.col("order_date").isNull(), F.lit("invalid order_date")),
                F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid quantity")),
                F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("invalid unit_price")),
            )
        )
        valid = deduped.filter(~invalid_condition)

        # Left joins intentionally retain unknown dimension keys for visibility.
        silver_orders = (
            valid.alias("o")
            .join(customers.select("customer_id", "customer_name", "city").alias("c"),
                  on="customer_id", how="left")
            .join(products.select("product_id", "product_name", "category").alias("p"),
                  on="product_id", how="left")
            .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
        )

        silver = WAREHOUSE_DIR / "silver"
        write_delta(silver_orders, silver / "orders")
        write_delta(rejected, silver / "rejected_orders")

        print("[4/4] Creating Gold business aggregates")
        daily_sales = (
            silver_orders.groupBy("order_date")
            .agg(
                F.sum("total_amount").alias("total_sales"),
                F.countDistinct("order_id").alias("total_orders"),
                F.sum("quantity").alias("units_sold"),
            )
        )
        product_sales = (
            silver_orders.groupBy("product_id", "product_name", "category")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.sum("quantity").alias("units_sold"),
            )
        )
        customer_sales = (
            silver_orders.groupBy("customer_id", "customer_name", "city")
            .agg(
                F.sum("total_amount").alias("lifetime_sales"),
                F.countDistinct("order_id").alias("order_count"),
            )
        )

        gold = WAREHOUSE_DIR / "gold"
        write_delta(daily_sales, gold / "daily_sales")
        write_delta(product_sales, gold / "product_sales")
        write_delta(customer_sales, gold / "customer_sales")

        input_count = orders.count()
        deduped_count = deduped.count()
        accepted_count = valid.count()
        rejected_count = rejected.count()
        duplicate_count = input_count - deduped_count

        quality_summary = spark.createDataFrame(
            [(input_count, accepted_count, rejected_count, duplicate_count)],
            ["input_records", "accepted_records", "rejected_records", "duplicate_records"],
        ).withColumn("run_timestamp", F.current_timestamp())
        write_delta(quality_summary, WAREHOUSE_DIR / "quality" / "summary")

        print("\nPipeline completed successfully.")
        print(f"Input records:     {input_count}")
        print(f"Accepted records:  {accepted_count}")
        print(f"Rejected records:  {rejected_count}")
        print(f"Duplicate records: {duplicate_count}")
        print(f"Output location:   {WAREHOUSE_DIR}")
        print("\nGold daily sales preview:")
        daily_sales.orderBy("order_date").show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
