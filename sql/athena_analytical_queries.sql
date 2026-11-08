-- ==============================================================================
-- Amazon Athena Analytical Queries (Cost-Optimized Partition Scans)
-- ==============================================================================

-- -----------------------------------------------------------------------------
-- Query 1: Total & Monthly Revenue
-- Calculates aggregate gross revenue, total completed sales, and average basket size per month
-- -----------------------------------------------------------------------------
SELECT
    year,
    month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_items_sold,
    ROUND(SUM(total_amount), 2) AS monthly_revenue,
    ROUND(AVG(total_amount), 2) AS average_order_value
FROM retail_analytics_db.fact_orders
WHERE order_status != 'Cancelled'
GROUP BY year, month
ORDER BY year, month;

-- -----------------------------------------------------------------------------
-- Query 2: Top 10 Products by Revenue
-- Identifies top commercial performers by total gross revenue and volume sold
-- -----------------------------------------------------------------------------
SELECT
    product_id,
    product_name,
    category,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(unit_price), 2) AS avg_unit_price
FROM retail_analytics_db.fact_orders
WHERE order_status != 'Cancelled'
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;

-- -----------------------------------------------------------------------------
-- Query 3: Regional Sales Performance
-- Analyzes territorial revenue, order volume, and active customer reach
-- -----------------------------------------------------------------------------
SELECT
    region,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(SUM(total_amount) / SUM(SUM(total_amount)) OVER () * 100, 2) AS revenue_share_pct
FROM retail_analytics_db.fact_orders
WHERE order_status != 'Cancelled'
GROUP BY region
ORDER BY total_revenue DESC;

-- -----------------------------------------------------------------------------
-- Query 4: Order Cancellation Rate
-- Measures operational fulfillment efficiency and cancellation ratios overall and by region
-- -----------------------------------------------------------------------------
SELECT
    region,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN order_status = 'Cancelled' THEN 1 END) AS cancelled_orders,
    COUNT(CASE WHEN order_status = 'Returned' THEN 1 END) AS returned_orders,
    COUNT(CASE WHEN order_status = 'Delivered' THEN 1 END) AS delivered_orders,
    ROUND(CAST(COUNT(CASE WHEN order_status = 'Cancelled' THEN 1 END) AS DOUBLE) / COUNT(*) * 100, 2) AS cancellation_rate_pct
FROM retail_analytics_db.fact_orders
GROUP BY region
ORDER BY cancellation_rate_pct DESC;

-- -----------------------------------------------------------------------------
-- Query 5: Customer Purchasing Trends (Frequency & Spend Segmentation)
-- Analyzes customer cohorts, repeat frequency, and customer lifetime value (LTV)
-- -----------------------------------------------------------------------------
WITH customer_aggregates AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS order_count,
        ROUND(SUM(total_amount), 2) AS lifetime_spend,
        ROUND(AVG(total_amount), 2) AS avg_basket_size,
        MIN(order_timestamp) AS first_order_date,
        MAX(order_timestamp) AS latest_order_date
    FROM retail_analytics_db.fact_orders
    WHERE order_status != 'Cancelled'
    GROUP BY customer_id
)
SELECT
    CASE
        WHEN lifetime_spend >= 1000 THEN 'VIP / High Value'
        WHEN lifetime_spend >= 400 THEN 'Mid Tier'
        ELSE 'Standard'
    END AS customer_tier,
    COUNT(customer_id) AS customer_count,
    ROUND(AVG(order_count), 2) AS avg_orders_per_customer,
    ROUND(SUM(lifetime_spend), 2) AS segment_total_revenue,
    ROUND(AVG(lifetime_spend), 2) AS avg_customer_ltv
FROM customer_aggregates
GROUP BY
    CASE
        WHEN lifetime_spend >= 1000 THEN 'VIP / High Value'
        WHEN lifetime_spend >= 400 THEN 'Mid Tier'
        ELSE 'Standard'
    END
ORDER BY segment_total_revenue DESC;
