with date_spine as (

    select date
    from unnest(
        generate_date_array('2024-01-01', '2027-12-31', interval 1 day)
    ) as date

),

final as (

    select
        -- surrogate key as integer for fast joins (e.g. 20250115)
        cast(format_date('%Y%m%d', date) as integer)        as date_id,

        -- the date itself
        date,

        -- day attributes
        extract(dayofweek from date)                         as day_of_week_number,
        format_date('%A', date)                              as day_of_week_name,
        extract(day from date)                               as day_of_month,

        -- week attributes
        extract(isoweek from date)                           as week_of_year,
        date_trunc(date, week(monday))                       as week_start_date,

        -- month attributes
        extract(month from date)                             as month_number,
        format_date('%B', date)                              as month_name,
        format_date('%b', date)                              as month_short_name,
        date_trunc(date, month)                              as month_start_date,

        -- quarter attributes
        extract(quarter from date)                           as calendar_quarter,
        date_trunc(date, quarter)                            as quarter_start_date,

        -- year attributes
        extract(year from date)                              as calendar_year,

        -- weekend flags
        extract(dayofweek from date) in (1, 7)               as is_weekend,
        extract(dayofweek from date) not in (1, 7)           as is_weekday,

        -- fiscal calendar (fiscal year starts February 1)
        case
            when extract(month from date) >= 2
            then extract(year from date)
            else extract(year from date) - 1
        end                                                  as fiscal_year,

        case
            when extract(month from date) in (2, 3, 4)   then 1
            when extract(month from date) in (5, 6, 7)   then 2
            when extract(month from date) in (8, 9, 10)  then 3
            when extract(month from date) in (11, 12, 1) then 4
        end                                                  as fiscal_quarter

    from date_spine

)

select * from final
