with source as (

    select * from {{ source('bronze', 'bronze_orders') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by order_id
            order by _loaded_at desc
        ) as row_num

    from source

),

renamed as (

    select
        -- primary keys
        cast(order_id as string)                            as order_id,
        cast(customer_id as string)                         as customer_id,

        -- order attributes
        cast(order_date as date)                            as order_date,
        cast(delivery_date as date)                         as delivery_date,
        lower(trim(status))                                 as status,
        cast(total_amount as numeric)                       as total_amount,

        -- derived columns
        date_diff(delivery_date, order_date, day)           as delivery_days,

        -- quality flags
        order_id is not null                                as has_order_id,
        customer_id is not null                             as has_customer_id,
        total_amount > 0                                    as is_positive_amount,
        delivery_date >= order_date                         as is_valid_delivery_date,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and order_id is not null

)

select * from renamed
