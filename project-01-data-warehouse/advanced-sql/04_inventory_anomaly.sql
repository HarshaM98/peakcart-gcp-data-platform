-- =============================================================================
-- Inventory Anomaly Detection
-- =============================================================================
-- Business question:
--   Which products had unusual day-over-day stock changes?
--   A change greater than 2 standard deviations from the mean is flagged
--   as an anomaly, indicating a potential data issue or supply chain event.
--
-- Tables used:
--   peakcart_gold.fact_daily_inventory
--   peakcart_gold.dim_products
--
-- Output:
--   Rows where qty_on_hand changed by more than 2 standard deviations,
--   with the actual change and z-score shown.
--
-- Key technique:
--   LAG() window function to get the previous day's quantity.
--   AVG() and STDDEV() window functions to calculate statistics per product.
--   Z-score = (value - mean) / stddev to normalise the change.
-- =============================================================================

WITH daily_changes AS (
    SELECT
        f.product_id,
        p.product_name,
        p.category,
        f.warehouse_id,
        f.snapshot_date,
        f.qty_on_hand,

        LAG(f.qty_on_hand) OVER (
            PARTITION BY f.product_id, f.warehouse_id
            ORDER BY f.snapshot_date
        )                                           AS prev_qty_on_hand,

        f.qty_on_hand - LAG(f.qty_on_hand) OVER (
            PARTITION BY f.product_id, f.warehouse_id
            ORDER BY f.snapshot_date
        )                                           AS qty_change

    FROM `harsha-data-platform.peakcart_gold.fact_daily_inventory` f
    JOIN `harsha-data-platform.peakcart_gold.dim_products` p
        ON f.product_surrogate_key = p.product_surrogate_key
),

change_stats AS (
    SELECT
        *,
        AVG(qty_change) OVER (
            PARTITION BY product_id, warehouse_id
        )                                           AS avg_daily_change,

        STDDEV(qty_change) OVER (
            PARTITION BY product_id, warehouse_id
        )                                           AS stddev_daily_change

    FROM daily_changes
    WHERE qty_change IS NOT NULL
),

anomalies AS (
    SELECT
        product_id,
        product_name,
        category,
        warehouse_id,
        snapshot_date,
        prev_qty_on_hand,
        qty_on_hand,
        qty_change,
        ROUND(avg_daily_change, 2)                  AS avg_daily_change,
        ROUND(stddev_daily_change, 2)               AS stddev_daily_change,
        ROUND(
            SAFE_DIVIDE(
                qty_change - avg_daily_change,
                stddev_daily_change
            ), 2
        )                                           AS z_score

    FROM change_stats
    WHERE stddev_daily_change > 0
)

SELECT
    product_id,
    product_name,
    category,
    warehouse_id,
    snapshot_date,
    prev_qty_on_hand,
    qty_on_hand,
    qty_change,
    z_score
FROM anomalies
WHERE ABS(z_score) > 2
ORDER BY ABS(z_score) DESC
LIMIT 15
