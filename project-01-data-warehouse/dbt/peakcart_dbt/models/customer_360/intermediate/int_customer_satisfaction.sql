with feedback as (

    select * from {{ ref('stg_delivery_feedback') }}

),

customer_satisfaction as (

    select
        customer_id,

        -- volume
        count(*)                                as total_feedback_count,
        countif(has_issue = true)               as total_issues_reported,
        countif(would_recommend = true)         as would_recommend_count,

        -- ratings
        round(avg(overall_rating), 2)           as avg_overall_rating,
        round(avg(driver_rating), 2)            as avg_driver_rating,
        round(avg(delivery_rating), 2)          as avg_delivery_rating,
        min(overall_rating)                     as min_overall_rating,

        -- recommendation rate
        round(
            countif(would_recommend = true)
            / nullif(count(*), 0)
            * 100,
            1
        )                                       as recommendation_rate_pct,

        -- issue breakdown
        countif(issue_type = 'late')            as late_delivery_count,
        countif(issue_type = 'damaged')         as damaged_count,
        countif(issue_type = 'wrong_items')     as wrong_items_count,
        countif(issue_type = 'missing_items')   as missing_items_count,

        -- nps bucket based on would_recommend rate
        case
            when countif(would_recommend = true)
                / nullif(count(*), 0) >= 0.8    then 'promoter'
            when countif(would_recommend = true)
                / nullif(count(*), 0) >= 0.6    then 'passive'
            else                                     'detractor'
        end                                     as nps_bucket,

        -- flag customers with no feedback at all
        -- (used in Gold to distinguish null from no data)
        false                                   as has_no_feedback

    from feedback
    group by customer_id

)

select * from customer_satisfaction
