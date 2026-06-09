-- =============================================================================
-- 7-Day Rolling Average Revenue per Category
-- =============================================================================
-- Business question:
--   What is the revenue trend per product category over time?
--   Daily revenue is noisy. Rolling averages reveal the underlying trend.
--
-- Tables used:
--   peakcart_gold.fact_orders
--   peakcart_gold.dim_products
--   peakcart_gold.dim_dates
--
-- Output:
--   One row per category per day with daily revenue and 7-day rolling average.
--
-- Key technique:
--   ROWS BETWEEN 6 PRECEDING AND CURRENT ROW defines a 7-row window frame.
--   PARTITION BY category ensures separate rolling calculation per category.
--   ROWS BETWEEN (physical rows) vs RANGE BETWEEN (value ranges):
--     Use ROWS for time series to avoid unexpected behaviour on tied dates.
-- =============================================================================

WITH daily_category_revenue AS (
    SELECT
        d.month_name,
        p.category,
        f.order_date,
        SUM(f.line_total)                           AS daily_revenue
    FROM `harsha-data-platform.peakcart_gold.fact_orders` f
    JOIN `harsha-data-platform.peakcart_gold.dim_products` p
        ON f.product_surrogate_key = p.product_surrogate_key
    JOIN `harsha-data-platform.peakcart_gold.dim_dates` d
        ON f.order_date = d.date
    GROUP BY
        d.month_name,
        p.category,
        f.order_date
),

rolling AS (
    SELECT
        category,
        order_date,
        month_name,
        ROUND(daily_revenue, 2)                     AS daily_revenue,

        ROUND(AVG(daily_revenue) OVER (
            PARTITION BY category
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2)                                       AS revenue_7d_rolling_avg,

        ROUND(SUM(daily_revenue) OVER (
            PARTITION BY category
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2)                                       AS revenue_7d_rolling_sum

    FROM daily_category_revenue
)

SELECT
    category,
    order_date,
    month_name,
    daily_revenue,
    revenue_7d_rolling_avg,
    revenue_7d_rolling_sum
FROM rolling
WHERE category = 'Produce'
ORDER BY order_date
LIMIT 15
