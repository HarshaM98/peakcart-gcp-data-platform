with source as (

    select * from {{ source('bronze_p03', 'bronze_customer_profiles') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by _loaded_at desc
        ) as row_num
    from source

),

renamed as (

    select
        cast(customer_id               as string)  as customer_id,
        cast(full_name                 as string)  as full_name,
        lower(trim(cast(email          as string))) as email,
        cast(city                      as string)  as city,
        cast(state                     as string)  as state,
        cast(signup_date               as date)    as signup_date,
        cast(preferred_delivery_window as string)  as preferred_delivery_window,
        cast(dietary_preferences       as string)  as dietary_preferences,
        cast(is_active                 as boolean) as is_active,

        -- quality flags
        email is not null
            and lower(trim(cast(email as string))) like '%@%.%'
                                                   as is_valid_email,
        signup_date <= current_date()              as is_valid_signup_date,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and customer_id is not null

)

select * from renamed
