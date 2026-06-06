with source as (

    select * from {{ source('bronze', 'bronze_products') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by product_id
            order by _loaded_at desc
        ) as row_num

    from source

),

renamed as (

    select
        -- primary keys
        cast(product_id as string)                          as product_id,
        cast(supplier_id as string)                         as supplier_id,

        -- product attributes
        trim(name)                                          as product_name,
        coalesce(trim(category), 'uncategorized')           as category,
        coalesce(trim(subcategory), 'uncategorized')        as subcategory,
        cast(price as numeric)                              as price,
        coalesce(is_active, false)                          as is_active,

        -- quality flags
        product_id is not null                              as has_product_id,
        price > 0                                           as is_positive_price,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and product_id is not null

)

select * from renamed
