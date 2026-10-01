with bounds as (
    select min(order_date) as min_d, max(ship_date) as max_d
    from {{ ref('stg_superstore') }}
),

days as (
    select cast(
        unnest(generate_series(
            cast(min_d as timestamp), cast(max_d as timestamp), interval 1 day
        )) as date
    ) as date_day
    from bounds
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day)       as year,
    quarter(date_day)    as quarter,
    month(date_day)      as month,
    monthname(date_day)  as month_name,
    day(date_day)        as day_of_month,
    dayname(date_day)    as day_name,
    isodow(date_day) in (6, 7) as is_weekend
from days