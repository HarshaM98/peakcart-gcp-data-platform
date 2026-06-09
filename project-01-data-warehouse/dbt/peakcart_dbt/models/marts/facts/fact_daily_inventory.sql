with inventory as (

    select * from {{ ref('stg_inventory_snapshots') }}

),

products as (

    select * from {{ ref('dim_products') }}
    where is_current = true

),

dates as (

    select * from {{ ref('dim_dates') }}

),

joined as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['i.snapshot_id']) }}
                                                    as inventory_surrogate_key,

        -- degenerate dimension
        i.snapshot_id,

        -- foreign keys
        p.product_surrogate_key,
        d.date_id                                   as snapshot_date_id,

        -- natural keys
        i.product_id,
        i.warehouse_id,
        i.snapshot_date,

        -- product context
        p.product_name,
        p.category,
        p.subcategory,

        -- inventory measures
        i.qty_on_hand,
        i.qty_reserved,
        i.qty_available,

        -- derived measures
        case
            when i.qty_available <= 0  then 'out_of_stock'
            when i.qty_available <= 10 then 'critical'
            when i.qty_available <= 50 then 'low'
            else                            'adequate'
        end                                         as stock_status,

        round(
            safe_divide(i.qty_reserved, i.qty_on_hand) * 100, 2
        )                                           as reservation_rate_pct,

        -- metadata
        i._loaded_at

    from inventory i

    inner join products p
        on i.product_id = p.product_id

    inner join dates d
        on i.snapshot_date = d.date

)

{% if is_incremental() %}

select * from joined
where snapshot_date >= date_sub(current_date(), interval 3 day)

{% else %}

select * from joined

{% endif %}
