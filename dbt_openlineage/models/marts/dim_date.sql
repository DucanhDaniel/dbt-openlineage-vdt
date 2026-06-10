with
    date_series as (
        select cast(d as date) as date_day
        from
            generate_series(
                '2022-01-01'::date, '2030-12-31'::date, '1 day'::interval
            ) as d
    )

select
    date_day as metric_date,
    extract(year from date_day) as date_year,
    extract(month from date_day) as date_month,
    extract(quarter from date_day) as date_quarter,
    extract(day from date_day) as date_day_of_month,
    extract(isodow from date_day) as date_day_of_week,
    to_char(date_day, 'YYYY-MM') as date_year_month,
    trim(to_char(date_day, 'Day')) as date_day_name,
    trim(to_char(date_day, 'Month')) as date_month_name
from date_series
