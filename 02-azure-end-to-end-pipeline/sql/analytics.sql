-- Run in Databricks SQL after the Gold layer has completed.
USE CATALOG retail_lakehouse;
USE SCHEMA retail;

-- Daily revenue and order volume
SELECT order_date, country, order_count, revenue, units_sold, average_order_value
FROM gold_daily_sales
ORDER BY order_date DESC, revenue DESC;

-- Top 10 products by revenue
SELECT product_id, product_name, category, units_sold, revenue, order_count
FROM gold_product_performance
ORDER BY revenue DESC
LIMIT 10;

-- Overall business KPIs
SELECT
  SUM(order_count) AS total_orders,
  SUM(revenue) AS total_revenue,
  SUM(units_sold) AS total_units,
  CASE WHEN SUM(order_count) = 0 THEN 0
       ELSE SUM(revenue) / SUM(order_count) END AS overall_average_order_value
FROM gold_daily_sales;
