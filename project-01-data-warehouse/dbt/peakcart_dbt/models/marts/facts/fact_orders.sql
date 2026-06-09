with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('dim_customers') }}

),

products as (

    select * from {{ ref('dim_products') }}

),

dates as (

    select * from {{ ref('dim_dates') }}

),

joined as (

    select
        -- surrogate key for the fact row
        {{ dbt_utils.generate_surrogate_key(['oi.order_item_id']) }}
                                                    as order_item_surrogate_key,

        -- degenerate dimensions
        oi.order_item_id,
        oi.order_id,

        -- foreign keys to dimensions
        c.customer_surrogate_key,
        p.product_surrogate_key,
        d.date_id                                   as order_date_id,

        -- natural keys for convenience
        oi.product_id,
        o.customer_id,

        -- order attributes
        o.order_date,
        o.delivery_date,
        o.status                                    as order_status,
        o.delivery_days,

        -- measures
        oi.quantity,
        oi.unit_price,
        oi.discount,
        oi.line_total,

        -- price variance
        p.price                                     as current_price,
        oi.unit_price                               as price_at_purchase,
        round(p.price - oi.unit_price, 2)           as price_variance,

        -- metadata
        oi._loaded_at

    from order_items oi

    inner join orders o
        on oi.order_id = o.order_id

    inner join customers c
        on o.customer_id = c.customer_id
        and o.order_date between c.valid_from and c.valid_to

    inner join products p
        on oi.product_id = p.product_id
        and o.order_date between p.valid_from and p.valid_to

    inner join dates d
        on o.order_date = d.date

)

{% if is_incremental() %}

select * from joined
where order_date >= date_sub(current_date(), interval 3 day)

{% else %}

select * from joined

{% endif %}
