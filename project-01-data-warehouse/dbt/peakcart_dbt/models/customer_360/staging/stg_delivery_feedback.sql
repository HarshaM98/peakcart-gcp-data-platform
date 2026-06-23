with source as (

    select * from {{ source('bronze_p03', 'bronze_delivery_feedback') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by feedback_id
            order by _loaded_at desc
        ) as row_num
    from source

),

renamed as (

    select
        cast(feedback_id      as string)  as feedback_id,
        cast(order_id         as string)  as order_id,
        cast(customer_id      as string)  as customer_id,
        cast(delivery_date    as date)    as delivery_date,
        cast(driver_rating    as integer) as driver_rating,
        cast(delivery_rating  as integer) as delivery_rating,
        cast(comment          as string)  as comment,
        cast(issue_type       as string)  as issue_type,
        cast(would_recommend  as boolean) as would_recommend,

        -- derived columns
        round(
            (cast(driver_rating   as numeric)
            + cast(delivery_rating as numeric)) / 2,
            1
        )                                 as overall_rating,

        comment is not null               as has_comment,
        issue_type != 'none'              as has_issue,

        -- quality flags
        driver_rating between 1 and 5     as is_valid_driver_rating,
        delivery_rating between 1 and 5   as is_valid_delivery_rating,

        -- metadata
        _loaded_at,
        _source_file

    from deduped
    where row_num = 1
      and feedback_id  is not null
      and customer_id  is not null

)

select * from renamed
