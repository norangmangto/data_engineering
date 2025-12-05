
  
  create view "weather"."main"."int_weather_features__dbt_tmp" as (
    with weather_data as (
    select * from "weather"."main"."stg_weather"
),

pivoted as (
    select
        date,
        max(case when parameter = 'temperature_air_mean_2m' then value end) as temp_mean,
        max(case when parameter = 'temperature_air_max_2m' then value end) as temp_max,
        max(case when parameter = 'temperature_air_min_2m' then value end) as temp_min,
        max(case when parameter = 'wind_speed' then value end) as wind_speed,
        max(case when parameter = 'precipitation_height' then value end) as precipitation,
        max(case when parameter = 'humidity' then value end) as humidity,
        max(case when parameter = 'pressure_air_site' then value end) as pressure_surface,
        max(case when parameter = 'sunshine_duration' then value end) as sunshine
    from weather_data
    group by date
),

cleaned as (
    select
        date,
        -- Simple interpolation or filling could be done here, but SQL window functions for interpolation are complex.
        -- We will assume data is mostly complete or handle nulls in training.
        -- For now, let's just use the values.
        temp_mean,
        temp_max,
        temp_min,
        wind_speed,
        coalesce(precipitation, 0) as precipitation, -- Assume 0 if null for precip
        humidity,
        sunshine,
        pressure_surface,
        -- Seasonality
        month(date) as month,
        dayofyear(date) as day_of_year,
        case when coalesce(precipitation, 0) > 0 then 1 else 0 end as is_raining
    from pivoted
),

features as (
    select
        *,
        -- Lag features (Past 1 day)
        lag(temp_mean, 1) over (order by date) as temp_mean_lag_1,
        lag(temp_max, 1) over (order by date) as temp_max_lag_1,
        lag(temp_min, 1) over (order by date) as temp_min_lag_1,
        lag(wind_speed, 1) over (order by date) as wind_speed_lag_1,
        lag(humidity, 1) over (order by date) as humidity_lag_1,
        lag(precipitation, 1) over (order by date) as precipitation_lag_1,
        lag(pressure_surface, 1) over (order by date) as pressure_surface_lag_1,

        -- Targets (Next 1 day)
        lead(temp_mean, 1) over (order by date) as target_temp_mean_day_1,
        lead(temp_max, 1) over (order by date) as target_temp_max_day_1,
        lead(temp_min, 1) over (order by date) as target_temp_min_day_1,
        lead(wind_speed, 1) over (order by date) as target_wind_speed_day_1,
        lead(humidity, 1) over (order by date) as target_humidity_day_1,
        lead(pressure_surface, 1) over (order by date) as target_pressure_surface_day_1,
        lead(month, 1) over (order by date) as target_month_day_1,
        lead(day_of_year, 1) over (order by date) as target_day_of_year_day_1,
        lead(is_raining, 1) over (order by date) as target_is_raining_day_1
    from cleaned
)

select * from features
where date < current_date -- Filter out future empty rows if any
  );
