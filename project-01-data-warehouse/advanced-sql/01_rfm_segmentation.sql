-- =============================================================================
-- RFM Segmentation
-- =============================================================================
-- Business question:
--   Which customers are our champions, at risk, or lost?
--   Score every customer on Recency, Frequency, and Monetary value
--   to enable targeted marketing campaigns.
--
-- Tables used:
--   peakcart_gold.fact_orders
--
-- Output:
--   One row per customer with rfm_score (e.g. 555 = champion, 111 = lost)
--   and a named segment for each customer.
--
-- Key technique:
--   NTILE(5) window function divides customers into 5 equal buckets per dimension.
--   Score 5 = best, Score 1 = worst.
-- =============================================================================

WITH customer_metrics AS (
    SELECT
        customer_id,
        MAX(order_date)                         AS last_order_date,
        COUNT(DISTINCT order_id)                AS order_count,
        SUM(line_total)                         AS total_spend
    FROM `harsha-data-platform.peakcart_gold.fact_orders`
    GROUP BY customer_id
),

rfm_scores AS (
    SELECT
        customer_id,
        last_order_date,
        order_count,
        ROUND(total_spend, 2)                   AS total_spend,

        NTILE(5) OVER (
            ORDER BY last_order_date DESC
        )                                       AS recency_score,

        NTILE(5) OVER (
            ORDER BY order_count ASC
        )                                       AS frequency_score,

        NTILE(5) OVER (
            ORDER BY total_spend ASC
        )                                       AS monetary_score

    FROM customer_metrics
),

rfm_segments AS (
    SELECT
        *,
        CONCAT(
            CAST(recency_score AS STRING),
            CAST(frequency_score AS STRING),
            CAST(monetary_score AS STRING)
        )                                       AS rfm_score,

        CASE
            WHEN recency_score >= 4
             AND frequency_score >= 4
             AND monetary_score >= 4  THEN 'champions'
            WHEN recency_score >= 3
             AND frequency_score >= 3  THEN 'loyal'
            WHEN recency_score >= 4
             AND frequency_score <= 2  THEN 'new_customer'
            WHEN recency_score <= 2
             AND frequency_score >= 3  THEN 'at_risk'
            WHEN recency_score = 1
             AND frequency_score = 1  THEN 'lost'
            ELSE                           'potential'
        END                                     AS segment

    FROM rfm_scores
)

SELECT
    segment,
    COUNT(*)                                    AS customer_count,
    ROUND(AVG(total_spend), 2)                  AS avg_spend,
    ROUND(AVG(order_count), 1)                  AS avg_orders,
    MIN(rfm_score)                              AS min_rfm,
    MAX(rfm_score)                              AS max_rfm
FROM rfm_segments
GROUP BY segment
ORDER BY avg_spend DESC
