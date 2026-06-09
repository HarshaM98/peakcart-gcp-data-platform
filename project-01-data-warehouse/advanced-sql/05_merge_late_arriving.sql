-- =============================================================================
-- MERGE Statement for Late-Arriving Inventory Data
-- =============================================================================
-- Business question:
--   Inventory snapshots sometimes arrive late or need correction.
--   This MERGE statement upserts new or corrected inventory records
--   into the fact table without creating duplicates.
--
-- Tables used:
--   peakcart_gold.fact_daily_inventory  (target)
--   peakcart_bronze.bronze_inventory_snapshots (source, simulated late arrivals)
--
-- Output:
--   Updated rows where snapshot_id already exists,
--   inserted rows for genuinely new snapshots.
--
-- Key technique:
--   MERGE matches on snapshot_id (the natural key).
--   WHEN MATCHED: update if qty values changed.
--   WHEN NOT MATCHED: insert the new row.
--   This is idempotent: running it twice produces the same result.
-- =============================================================================

MERGE `harsha-data-platform.peakcart_gold.fact_daily_inventory` AS target
USING (
    SELECT
        CAST(snapshot_id AS STRING)             AS snapshot_id,
        CAST(product_id AS STRING)              AS product_id,
        warehouse_id,
        snapshot_date,
        qty_on_hand,
        qty_reserved,
        qty_on_hand - qty_reserved              AS qty_available
    FROM `harsha-data-platform.peakcart_bronze.bronze_inventory_snapshots`
) AS source
ON target.snapshot_id = source.snapshot_id

WHEN MATCHED AND (
    target.qty_on_hand  <> source.qty_on_hand OR
    target.qty_reserved <> source.qty_reserved
) THEN UPDATE SET
    target.qty_on_hand      = source.qty_on_hand,
    target.qty_reserved     = source.qty_reserved,
    target.qty_available    = source.qty_available

WHEN NOT MATCHED BY TARGET THEN INSERT (
    snapshot_id,
    product_id,
    warehouse_id,
    snapshot_date,
    qty_on_hand,
    qty_reserved,
    qty_available
)
VALUES (
    source.snapshot_id,
    source.product_id,
    source.warehouse_id,
    source.snapshot_date,
    source.qty_on_hand,
    source.qty_reserved,
    source.qty_available
)
