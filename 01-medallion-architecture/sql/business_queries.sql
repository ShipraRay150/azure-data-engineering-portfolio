-- Run these queries in a Spark SQL / Databricks SQL environment after
-- registering the generated Delta folders as tables or views.

-- 1. Daily sales trend
SELECT order_date, total_sales, total_orders, units_sold
FROM gold.daily_sales
ORDER BY order_date;

-- 2. Top products by revenue
SELECT product_id, product_name, category, total_revenue, units_sold
FROM gold.product_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Top customers by lifetime sales
SELECT customer_id, customer_name, city, lifetime_sales, order_count
FROM gold.customer_sales
ORDER BY lifetime_sales DESC
LIMIT 10;
