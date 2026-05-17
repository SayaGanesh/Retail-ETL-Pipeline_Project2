-- ============================================================
-- Retail ETL – Synapse Analytics DDL & BI Views
-- ============================================================

-- ------------------------------------------------------------
-- 1. External Table: fact_sales (Delta Lake)
-- ------------------------------------------------------------
CREATE EXTERNAL TABLE fact_sales (
    order_id         NVARCHAR(50),
    order_date       DATE,
    year             INT,
    month            INT,
    day              INT,
    customer_id      NVARCHAR(50),
    product_id       NVARCHAR(50),
    product_category NVARCHAR(100),
    product_brand    NVARCHAR(100),
    store_id         NVARCHAR(50),
    channel          NVARCHAR(50),
    source_system    NVARCHAR(50),
    quantity         INT,
    unit_price       DECIMAL(18,2),
    discount_pct     DECIMAL(5,2),
    net_revenue      DECIMAL(18,2)
)
WITH (
    LOCATION = 'gold/fact_sales/',
    DATA_SOURCE = gold_retail_storage,
    FILE_FORMAT = delta_format
);

-- ------------------------------------------------------------
-- 2. View: Monthly Revenue by Category
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_monthly_revenue_by_category AS
SELECT
    year,
    month,
    product_category,
    SUM(net_revenue)        AS total_revenue,
    SUM(quantity)           AS units_sold,
    COUNT(DISTINCT order_id) AS order_count,
    ROUND(AVG(net_revenue), 2) AS avg_order_value
FROM fact_sales
GROUP BY year, month, product_category
ORDER BY year DESC, month DESC, total_revenue DESC;

-- ------------------------------------------------------------
-- 3. View: Channel Performance Comparison
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_channel_performance AS
SELECT
    channel,
    source_system,
    COUNT(DISTINCT order_id)   AS total_orders,
    SUM(net_revenue)           AS total_revenue,
    ROUND(AVG(discount_pct * 100), 2) AS avg_discount_pct,
    ROUND(AVG(net_revenue), 2) AS avg_order_value
FROM fact_sales
GROUP BY channel, source_system
ORDER BY total_revenue DESC;

-- ------------------------------------------------------------
-- 4. View: Top 20 Products by Revenue
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_top_products AS
SELECT TOP 20
    product_id,
    product_category,
    product_brand,
    SUM(net_revenue)    AS total_revenue,
    SUM(quantity)       AS total_units,
    COUNT(order_id)     AS order_count,
    ROUND(AVG(unit_price), 2) AS avg_selling_price
FROM fact_sales
GROUP BY product_id, product_category, product_brand
ORDER BY total_revenue DESC;

-- ------------------------------------------------------------
-- 5. Revenue Growth Month-over-Month (Window Function)
-- ------------------------------------------------------------
SELECT
    year,
    month,
    SUM(net_revenue) AS monthly_revenue,
    LAG(SUM(net_revenue)) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (SUM(net_revenue) - LAG(SUM(net_revenue)) OVER (ORDER BY year, month))
        / NULLIF(LAG(SUM(net_revenue)) OVER (ORDER BY year, month), 0) * 100,
        2
    ) AS mom_growth_pct
FROM fact_sales
GROUP BY year, month
ORDER BY year, month;

-- ------------------------------------------------------------
-- 6. Customer Repeat Purchase Rate
-- ------------------------------------------------------------
SELECT
    customer_id,
    COUNT(DISTINCT order_id)   AS total_orders,
    MIN(order_date)            AS first_purchase,
    MAX(order_date)            AS last_purchase,
    DATEDIFF(DAY, MIN(order_date), MAX(order_date)) AS customer_lifespan_days,
    SUM(net_revenue)           AS lifetime_value
FROM fact_sales
GROUP BY customer_id
HAVING COUNT(DISTINCT order_id) > 1
ORDER BY lifetime_value DESC;
