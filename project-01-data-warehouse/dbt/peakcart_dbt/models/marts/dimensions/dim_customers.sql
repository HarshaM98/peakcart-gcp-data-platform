with snapshot as (

    select * from {{ ref('snap_customers') }}

),

final as (

    select
        -- surrogate key, unique per customer per version
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }}
                                                as customer_surrogate_key,

        -- natural key
        customer_id,

        -- customer attributes
        customer_name,
        email,
        city,
        state,
        signup_date,
        segment,

        -- quality flags carried forward
        is_valid_email,
        is_valid_signup_date,

        -- SCD Type 2 columns
        cast(dbt_valid_from as date)            as valid_from,
        case
            when dbt_valid_to is null
            then date('9999-12-31')
            else cast(dbt_valid_to as date)
        end                                     as valid_to,
        dbt_valid_to is null                    as is_current,

        -- metadata
        dbt_updated_at                          as last_updated_at

    from snapshot

)

select * from final
