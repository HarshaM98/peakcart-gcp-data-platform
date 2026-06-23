with customers as (

    select * from {{ ref('stg_customer_profiles') }}

),

order_metrics as (

    select * from {{ ref('int_customer_order_metrics') }}

),

satisfaction as (

    select * from {{ ref('int_customer_satisfaction') }}

),

session_metrics as (

    -- aggregate int_clickstream_sessions to customer level
    select
        customer_id,

        count(*)                                as total_sessions,
        round(avg(session_duration_mins), 1)    as avg_session_duration_mins,
        round(avg(total_events), 1)             as avg_events_per_session,
        round(avg(page_views), 1)               as avg_page_views_per_session,
        round(avg(product_views), 1)            as avg_product_views_per_session,
        sum(add_to_cart_count)                  as total_add_to_cart,
        countif(did_purchase = true)            as sessions_with_purchase,
        countif(cart_abandoned = true)          as sessions_with_abandonment,

        round(
            countif(cart_abandoned = true)
            / nullif(count(*), 0)
            * 100,
            1
        )                                       as cart_abandonment_rate_pct,

        -- most used device across all sessions
        approx_top_count(device_type, 1)[offset(0)].value
                                                as preferred_device

    from {{ ref('int_clickstream_sessions') }}
    group by customer_id

),

rfm_scores as (

    -- RFM scoring using NTILE(5) on delivered customers only
    -- Score 5 = best, Score 1 = worst for all three dimensions
    select
        customer_id,

        -- Recency: lower days_since_last_order = better = higher score
        ntile(5) over (
            order by days_since_last_order asc
        )                                       as recency_score,

        -- Frequency: higher total_orders = better = higher score
        ntile(5) over (
            order by total_orders asc
        )                                       as frequency_score,

        -- Monetary: higher total_spend = better = higher score
        ntile(5) over (
            order by total_spend asc
        )                                       as monetary_score

    from order_metrics

),

rfm_segments as (

    select
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        concat(
            cast(recency_score  as string),
            cast(frequency_score as string),
            cast(monetary_score  as string)
        )                                       as rfm_score,

        -- segment label based on combined score
        case
            when recency_score >= 4
                and frequency_score >= 4
                and monetary_score  >= 4        then 'champion'
            when recency_score >= 3
                and frequency_score >= 3        then 'loyal'
            when recency_score >= 4
                and frequency_score <= 2        then 'new_customer'
            when recency_score <= 2
                and frequency_score >= 3        then 'at_risk'
            when recency_score <= 2
                and frequency_score <= 2        then 'lost'
            else                                     'potential'
        end                                     as rfm_segment

    from rfm_scores

),

joined as (

    -- left join everything from the customer spine outward
    -- customers with no sessions or no feedback still appear
    select
        -- identity
        c.customer_id,
        c.full_name,
        c.email,
        c.city,
        c.state,
        c.signup_date,
        c.preferred_delivery_window,
        c.dietary_preferences,
        c.is_active,
        c.is_valid_email,

        -- cohort
        date_diff(current_date(), c.signup_date, day)
                                                as days_since_signup,
        case
            when date_diff(current_date(), c.signup_date, day) <= 90
                                                then 'new'
            when date_diff(current_date(), c.signup_date, day) <= 365
                                                then 'established'
            else                                     'veteran'
        end                                     as customer_cohort,

        -- order metrics (all 1000 customers have this)
        coalesce(o.total_orders, 0)             as total_orders,
        coalesce(o.delivered_orders, 0)         as delivered_orders,
        coalesce(o.cancelled_orders, 0)         as cancelled_orders,
        coalesce(o.total_spend, 0)              as total_spend,
        o.avg_order_value,
        o.max_order_value,
        o.first_order_date,
        o.last_order_date,
        o.days_since_last_order,
        coalesce(o.cancellation_rate_pct, 0)    as cancellation_rate_pct,

        -- rfm
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_score,
        r.rfm_segment,

        -- session metrics (955 customers have this)
        coalesce(s.total_sessions, 0)           as total_sessions,
        s.avg_session_duration_mins,
        s.avg_events_per_session,
        s.avg_page_views_per_session,
        s.avg_product_views_per_session,
        coalesce(s.total_add_to_cart, 0)        as total_add_to_cart,
        coalesce(s.sessions_with_purchase, 0)   as sessions_with_purchase,
        coalesce(s.sessions_with_abandonment, 0) as sessions_with_abandonment,
        s.cart_abandonment_rate_pct,
        s.preferred_device,

        -- satisfaction (741 customers have this)
        sat.total_feedback_count,
        sat.avg_overall_rating,
        sat.avg_driver_rating,
        sat.avg_delivery_rating,
        sat.recommendation_rate_pct,
        sat.nps_bucket,
        sat.total_issues_reported,

        -- flag customers with no feedback (null vs no data)
        sat.customer_id is null                 as has_no_feedback,

        -- metadata
        current_timestamp()                     as model_updated_at

    from customers            c
    left join order_metrics   o   on c.customer_id = o.customer_id
    left join rfm_segments    r   on c.customer_id = r.customer_id
    left join session_metrics s   on c.customer_id = s.customer_id
    left join satisfaction    sat on c.customer_id = sat.customer_id

)

select * from joined
