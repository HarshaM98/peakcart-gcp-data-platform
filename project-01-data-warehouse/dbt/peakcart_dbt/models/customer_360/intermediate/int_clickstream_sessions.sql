with events_with_gap as (

    select
        event_id,
        customer_id,
        session_id          as raw_session_id,
        event_type,
        product_id,
        page_url,
        event_timestamp,
        device_type,

        timestamp_diff(
            event_timestamp,
            lag(event_timestamp) over (
                partition by customer_id
                order by event_timestamp
            ),
            minute
        )                   as minutes_since_last_event

    from {{ ref('stg_clickstream_events') }}
    where customer_id is not null

),

events_with_session_flag as (

    select
        *,
        case
            when minutes_since_last_event is null then 1
            when minutes_since_last_event > 30    then 1
            else 0
        end                 as is_session_start

    from events_with_gap

),

events_with_session_number as (

    select
        *,
        sum(is_session_start) over (
            partition by customer_id
            order by event_timestamp
            rows between unbounded preceding and current row
        )                   as session_number

    from events_with_session_flag

),

events_with_true_session_id as (

    select
        *,
        to_hex(md5(concat(
            customer_id, '-',
            cast(session_number as string)
        )))                 as true_session_id

    from events_with_session_number

),

sessions_aggregated as (

    select
        true_session_id,
        customer_id,

        min(event_timestamp)                    as session_start_time,
        max(event_timestamp)                    as session_end_time,

        timestamp_diff(
            max(event_timestamp),
            min(event_timestamp),
            minute
        )                                       as session_duration_mins,

        count(*)                                as total_events,
        countif(event_type = 'page_view')       as page_views,
        countif(event_type = 'product_view')    as product_views,
        countif(event_type = 'add_to_cart')     as add_to_cart_count,
        countif(event_type = 'search')          as search_count,
        countif(event_type = 'purchase')        as purchase_count,

        countif(event_type = 'purchase') > 0    as did_purchase,

        countif(event_type = 'add_to_cart') > 0
            and countif(event_type = 'purchase') = 0
                                                as cart_abandoned,

        approx_top_count(device_type, 1)[offset(0)].value
                                                as device_type,

        -- fix: GREATEST guards against -1 on search-only sessions
        greatest(
            count(distinct product_id) - 1, 0
        )                                       as unique_products_viewed

    from events_with_true_session_id
    group by true_session_id, customer_id

)

select * from sessions_aggregated
