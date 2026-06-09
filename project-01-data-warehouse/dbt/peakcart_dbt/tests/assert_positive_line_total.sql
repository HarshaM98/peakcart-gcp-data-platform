-- Assert that all line totals are positive.
-- A negative line total indicates a negative quantity in the source,
-- which may represent a return or adjustment from the source system.
-- Configured as a warning (not error) because negative quantities
-- are a known source data characteristic, not a pipeline bug.
-- Returns failing rows. Test passes when zero rows are returned.

{{ config(severity='warn') }}

SELECT
    order_item_id,
    quantity,
    unit_price,
    discount,
    line_total
FROM {{ ref('stg_order_items') }}
WHERE line_total <= 0
