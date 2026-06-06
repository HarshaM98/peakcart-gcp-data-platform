with source as (

    select * from {{ ref('stg_suppliers') }}

),

final as (

    select
        -- primary key
        supplier_id,

        -- supplier attributes
        supplier_name,
        region,
        lead_time_days,
        is_active,

        -- derived attributes
        case
            when lead_time_days <= 3  then 'fast'
            when lead_time_days <= 7  then 'standard'
            when lead_time_days <= 14 then 'slow'
            else 'very_slow'
        end                             as lead_time_tier,

        -- metadata
        _loaded_at

    from source

)

select * from final
