-- =============================================================================
-- Cohort Analysis: Customer Retention by Signup Month
-- =============================================================================
-- Business question:
--   Of customers who signed up in a given month, what percentage are still
--   ordering at 30, 60, 90, and 180 days after signup?
--   This measures how well the business retains new customers over time.
--
-- Tables used:
--   peakcart_gold.fact_orders
--   peakcart_gold.dim_customers
--
-- Output:
--   One row per cohort month showing retention rates at each interval.
--
-- Key technique:
--   DATE_DIFF to calculate days between signup and each order.
--   COUNTIF to count customers who ordered within each window.
--   Divide by cohort size to get retention rate percentage.
-- =============================================================================

WITH customer_cohorts AS (
    SELECT
        c.customer_id,
        c.signup_date,
        DATE_TRUNC(c.signup_date, MONTH)            AS cohort_month,
        MIN(f.order_date)                           AS first_order_date
    FROM `harsha-data-platform.peakcart_gold.dim_customers` c
    LEFT JOIN `harsha-data-platform.peakcart_gold.fact_orders` f
        ON c.customer_id = f.customer_id
    WHERE c.is_current = true
    GROUP BY
        c.customer_id,
        c.signup_date,
        cohort_month
),

cohort_orders AS (
    SELECT
        co.customer_id,
        co.cohort_month,
        co.signup_date,
        f.order_date,
        DATE_DIFF(f.order_date, co.signup_date, DAY) AS days_since_signup
    FROM customer_cohorts co
    LEFT JOIN `harsha-data-platform.peakcart_gold.fact_orders` f
        ON co.customer_id = f.customer_id
),

cohort_retention AS (
    SELECT
        cohort_month,
        COUNT(DISTINCT customer_id)                 AS cohort_size,

        COUNT(DISTINCT CASE
            WHEN days_since_signup BETWEEN 0 AND 30
            THEN customer_id END)                   AS retained_30d,

        COUNT(DISTINCT CASE
            WHEN days_since_signup BETWEEN 0 AND 60
            THEN customer_id END)                   AS retained_60d,

        COUNT(DISTINCT CASE
            WHEN days_since_signup BETWEEN 0 AND 90
            THEN customer_id END)                   AS retained_90d,

        COUNT(DISTINCT CASE
            WHEN days_since_signup BETWEEN 0 AND 180
            THEN customer_id END)                   AS retained_180d

    FROM cohort_orders
    GROUP BY cohort_month
)

SELECT
    FORMAT_DATE('%Y-%m', cohort_month)              AS cohort_month,
    cohort_size,
    retained_30d,
    ROUND(retained_30d / cohort_size * 100, 1)      AS retention_30d_pct,
    ROUND(retained_60d / cohort_size * 100, 1)      AS retention_60d_pct,
    ROUND(retained_90d / cohort_size * 100, 1)      AS retention_90d_pct,
    ROUND(retained_180d / cohort_size * 100, 1)     AS retention_180d_pct
FROM cohort_retention
ORDER BY cohort_month
LIMIT 12
