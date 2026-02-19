/*
    dim_date

    Generated date spine for 2008-2010.
*/

with date_spine as (
    select range::date as full_date
    from range(date '2008-01-01', date '2011-01-01', interval 1 day)
),

final as (
    select
        cast(strftime(full_date, '%Y%m%d') as integer) as date_key,
        full_date,
        year(full_date) as year,
        month(full_date) as month,
        day(full_date) as day,
        quarter(full_date) as quarter,
        strftime(full_date, '%B') as month_name,
        strftime(full_date, '%A') as day_name,
        case when strftime(full_date, '%u') in ('6', '7') then true else false end as is_weekend
    from date_spine
)

select * from final
