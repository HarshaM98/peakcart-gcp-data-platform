with source as (

    select * from {{ source('bronze', 'bronze_product_price_history') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by price_id
            order by _loaded_at desc
        ) as row_num

    from source

),

renamed as (

    select
        -- primary key
        cast(price_id as string)                            as price_id,
        cast(product_id as string)                          as product_id,

        -- price attributes
        cast(price as numeric)                              as price,
        coalesce(trim(change_reason), 'unknown')            as change_reason,

        -- SCD Type 2 date range columns
        cast(effective_date as date)                        as valid_from,
        cast(
            coalesce(end_date, date('9999-12-31'))
        as date)                                            as valid_to,

        -- current record flag
        end_date is null                                    as is_current,

        -- quality flags
        price_id is not null                                as has_price_id,
        product_id is not null                              as has_product_id,
        price > 0                                           as is_positive_price,
        effective_date is not null                          as has_valid_from,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and price_id is not null
      and product_id is not null

)

select * from renamed
