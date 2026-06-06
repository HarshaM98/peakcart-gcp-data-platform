with source as (

    select * from {{ source('bronze', 'bronze_suppliers') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by supplier_id
            order by _loaded_at desc
        ) as row_num

    from source

),

renamed as (

    select
        -- primary key
        cast(supplier_id as string)                         as supplier_id,

        -- supplier attributes
        trim(name)                                          as supplier_name,
        coalesce(trim(region), 'unknown')                   as region,
        cast(lead_time_days as integer)                     as lead_time_days,
        coalesce(is_active, false)                          as is_active,

        -- quality flags
        supplier_id is not null                             as has_supplier_id,
        lead_time_days > 0                                  as is_positive_lead_time,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and supplier_id is not null

)

select * from renamed
