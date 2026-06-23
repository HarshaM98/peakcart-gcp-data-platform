with source as (

    select * from {{ source('bronze_p03', 'bronze_clickstream_events') }}

),

deduped as (

    -- Deduplicate on event_id only.
    -- A customer genuinely can view the same product twice.
    -- We only remove exact duplicate rows from double-publishing.
    select
        *,
        row_number() over (
            partition by event_id
            order by _loaded_at desc
        ) as row_num
    from source

),

renamed as (

    select
        cast(event_id        as string)    as event_id,
        cast(customer_id     as string)    as customer_id,
        cast(session_id      as string)    as session_id,
        cast(event_type      as string)    as event_type,
        cast(product_id      as string)    as product_id,
        cast(page_url        as string)    as page_url,
        cast(search_query    as string)    as search_query,
        cast(event_timestamp as timestamp) as event_timestamp,
        cast(device_type     as string)    as device_type,

        -- derived columns
        date(cast(event_timestamp as timestamp))  as event_date,
        extract(hour from
            cast(event_timestamp as timestamp))   as event_hour,

        -- quality flags
        customer_id is not null               as is_known_customer,
        event_type in (
            'page_view', 'product_view',
            'add_to_cart', 'search',
            'purchase', 'remove_from_cart'
        )                                     as is_valid_event_type,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and event_id is not null

)

select * from renamed
