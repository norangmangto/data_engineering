/*
    dim_date

    Date dimension generated from a date spine covering the full
    range of order dates in the dataset (2016-01-01 to 2018-12-31).
*/

with date_spine as (
    select
        unnest(
            generate_series(
                date '2016-01-01',
                date '2018-12-31',
                interval '1 day'
            )
        ) as full_date
),

final as (
    select
        cast(strftime(full_date, '%Y%m%d') as int) as date_key,
        cast(full_date as date)                     as full_date,
        extract(year from full_date)                as year,
        extract(quarter from full_date)             as quarter,
        extract(month from full_date)               as month,
        extract(day from full_date)                 as day,
        extract(dow from full_date)                 as day_of_week,
        strftime(full_date, '%B')                   as month_name,
        strftime(full_date, '%A')                   as day_name,
        case
            when extract(dow from full_date) in (0, 6) then true
            else false
        end as is_weekend
    from date_spine
)

select * from final
