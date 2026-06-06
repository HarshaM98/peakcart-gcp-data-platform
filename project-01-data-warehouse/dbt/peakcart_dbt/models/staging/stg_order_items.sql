with source as (

    select * from {{ source('bronze', 'bronze_order_items') }}

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
        -- primary keys
        cast(order_item_id as string)                       as order_item_id,
        cast(order_id as string)                            as order_id,
        cast(product_id as string)                          as product_id,

        -- item attributes
        cast(quantity as integer)                           as quantity,
        cast(unit_price as numeric)                         as unit_price,
        coalesce(cast(discount as numeric), 0)              as discount,

        -- derived columns
        cast(quantity as numeric)
            * cast(unit_price as numeric)
            * (1 - coalesce(cast(discount as numeric), 0))  as line_total,

        -- quality flags
        order_item_id is not null                           as has_order_item_id,
        quantity > 0                                        as is_positive_quantity,
        unit_price > 0                                      as is_positive_price,
        discount >= 0 and discount <= 1                     as is_valid_discount,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and order_item_id is not null

)

select * from renamed
