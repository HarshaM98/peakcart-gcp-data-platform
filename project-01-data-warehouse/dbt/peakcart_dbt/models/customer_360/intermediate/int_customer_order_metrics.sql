with orders as (

    select * from {{ ref('stg_order_history') }}

),

-- aggregate at order level first (not item level)
-- to avoid double-counting order-level metrics
order_level as (

    select
        order_id,
        customer_id,
        order_date,
        order_status,
        sum(line_total)     as order_total,
        count(*)            as items_in_order

    from orders
    group by order_id, customer_id, order_date, order_status

),

-- now aggregate to customer level
customer_order_metrics as (

    select
        customer_id,

        -- order counts
        count(distinct order_id)                as total_orders,
        countif(order_status = 'delivered')     as delivered_orders,
        countif(order_status = 'cancelled')     as cancelled_orders,

        -- spend metrics (delivered orders only for accuracy)
        round(sum(
            case when order_status = 'delivered'
            then order_total else 0 end
        ), 2)                                   as total_spend,

        round(avg(
            case when order_status = 'delivered'
            then order_total else null end
        ), 2)                                   as avg_order_value,

        round(max(
            case when order_status = 'delivered'
            then order_total else null end
        ), 2)                                   as max_order_value,

        -- recency
        min(order_date)                         as first_order_date,
        max(order_date)                         as last_order_date,
        date_diff(
            current_date(),
            max(order_date),
            day
        )                                       as days_since_last_order,

        -- favorite category proxy (most ordered product)
        -- we do not have category here, using order frequency instead
        round(
            countif(order_status = 'cancelled')
            / nullif(count(distinct order_id), 0)
            * 100,
            1
        )                                       as cancellation_rate_pct

    from order_level
    group by customer_id

)

select * from customer_order_metrics
