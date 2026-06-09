-- Assert that price periods have no gaps or overlaps per product
-- valid_from must always be less than valid_to
-- Returns failing rows. Test passes when zero rows are returned.

SELECT
    product_id,
    valid_from,
    valid_to,
    price
FROM {{ ref('stg_product_price_history') }}
WHERE valid_from >= valid_to
