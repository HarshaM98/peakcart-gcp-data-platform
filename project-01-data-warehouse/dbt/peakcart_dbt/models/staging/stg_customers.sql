with source as (

    select * from {{ source('bronze', 'bronze_customers') }}

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
        -- primary key
        cast(customer_id as string)                         as customer_id,

        -- customer attributes
        trim(name)                                          as customer_name,
        lower(trim(email))                                  as email,
        trim(city)                                          as city,
        trim(state)                                         as state,
        cast(signup_date as date)                           as signup_date,
        coalesce(trim(segment), 'unknown')                  as segment,

        -- quality flags
        email like '%@%.%'                                  as is_valid_email,
        signup_date <= current_date()                       as is_valid_signup_date,
        customer_id is not null                             as has_customer_id,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and customer_id is not null

)

select * from renamed
