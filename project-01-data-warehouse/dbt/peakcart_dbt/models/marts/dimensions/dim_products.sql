with products as (

    select * from {{ ref('stg_products') }}

),

price_history as (

    select * from {{ ref('stg_product_price_history') }}

),

final as (

    select
        -- surrogate key, unique per product per price period
        {{ dbt_utils.generate_surrogate_key(['p.product_id', 'ph.valid_from']) }}
                                                as product_surrogate_key,

        -- natural key
        p.product_id,

        -- product attributes
        p.product_name,
        p.category,
        p.subcategory,
        p.supplier_id,
        p.is_active,

        -- price history (SCD Type 2)
        ph.price,
        ph.change_reason                        as price_change_reason,
        ph.valid_from,
        ph.valid_to,
        ph.is_current,

        -- metadata
        p._loaded_at

    from products p
    inner join price_history ph
        on p.product_id = ph.product_id

)

select * from final
