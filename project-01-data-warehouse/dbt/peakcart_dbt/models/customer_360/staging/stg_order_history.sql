with source as (

    select * from {{ source('bronze_p03', 'bronze_order_history') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by order_item_id
            order by _loaded_at desc
        ) as row_num
    from source

),

renamed as (

    select
        cast(order_item_id   as string)  as order_item_id,
        cast(order_id        as string)  as order_id,
        cast(customer_id     as string)  as customer_id,
        cast(product_id      as string)  as product_id,
        cast(order_date      as date)    as order_date,
        cast(delivery_date   as date)    as delivery_date,
        cast(order_status    as string)  as order_status,
        cast(quantity        as integer) as quantity,
        cast(unit_price      as numeric) as unit_price,
        cast(discount        as numeric) as discount,
        cast(delivery_rating as integer) as delivery_rating,

        -- derived columns
        date_diff(
            cast(delivery_date as date),
            cast(order_date as date),
            day
        )                                as delivery_days,

        round(
            cast(quantity  as numeric)
            * cast(unit_price as numeric)
            * (1 - cast(discount as numeric)),
            2
        )                                as line_total,

        -- quality flags
        delivery_date >= order_date      as is_valid_delivery_date,
        quantity > 0                     as is_positive_quantity,
        unit_price > 0                   as is_positive_price,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and order_item_id is not null
      and customer_id   is not null

)

select * from renamed
