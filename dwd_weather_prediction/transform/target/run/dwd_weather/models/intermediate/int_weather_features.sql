
  
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
        -- Targets (Next 7 days)
        
        lead(temp_mean, 1) over (order by date) as target_temp_mean_day_1,
        lead(temp_max, 1) over (order by date) as target_temp_max_day_1,
        lead(temp_min, 1) over (order by date) as target_temp_min_day_1,
        lead(wind_speed, 1) over (order by date) as target_wind_speed_day_1,
        lead(humidity, 1) over (order by date) as target_humidity_day_1,
        lead(pressure_surface, 1) over (order by date) as target_pressure_surface_day_1,
        lead(is_raining, 1) over (order by date) as target_is_raining_day_1,
        
        lead(temp_mean, 2) over (order by date) as target_temp_mean_day_2,
        lead(temp_max, 2) over (order by date) as target_temp_max_day_2,
        lead(temp_min, 2) over (order by date) as target_temp_min_day_2,
        lead(wind_speed, 2) over (order by date) as target_wind_speed_day_2,
        lead(humidity, 2) over (order by date) as target_humidity_day_2,
        lead(pressure_surface, 2) over (order by date) as target_pressure_surface_day_2,
        lead(is_raining, 2) over (order by date) as target_is_raining_day_2,
        
        lead(temp_mean, 3) over (order by date) as target_temp_mean_day_3,
        lead(temp_max, 3) over (order by date) as target_temp_max_day_3,
        lead(temp_min, 3) over (order by date) as target_temp_min_day_3,
        lead(wind_speed, 3) over (order by date) as target_wind_speed_day_3,
        lead(humidity, 3) over (order by date) as target_humidity_day_3,
        lead(pressure_surface, 3) over (order by date) as target_pressure_surface_day_3,
        lead(is_raining, 3) over (order by date) as target_is_raining_day_3,
        
        lead(temp_mean, 4) over (order by date) as target_temp_mean_day_4,
        lead(temp_max, 4) over (order by date) as target_temp_max_day_4,
        lead(temp_min, 4) over (order by date) as target_temp_min_day_4,
        lead(wind_speed, 4) over (order by date) as target_wind_speed_day_4,
        lead(humidity, 4) over (order by date) as target_humidity_day_4,
        lead(pressure_surface, 4) over (order by date) as target_pressure_surface_day_4,
        lead(is_raining, 4) over (order by date) as target_is_raining_day_4,
        
        lead(temp_mean, 5) over (order by date) as target_temp_mean_day_5,
        lead(temp_max, 5) over (order by date) as target_temp_max_day_5,
        lead(temp_min, 5) over (order by date) as target_temp_min_day_5,
        lead(wind_speed, 5) over (order by date) as target_wind_speed_day_5,
        lead(humidity, 5) over (order by date) as target_humidity_day_5,
        lead(pressure_surface, 5) over (order by date) as target_pressure_surface_day_5,
        lead(is_raining, 5) over (order by date) as target_is_raining_day_5,
        
        lead(temp_mean, 6) over (order by date) as target_temp_mean_day_6,
        lead(temp_max, 6) over (order by date) as target_temp_max_day_6,
        lead(temp_min, 6) over (order by date) as target_temp_min_day_6,
        lead(wind_speed, 6) over (order by date) as target_wind_speed_day_6,
        lead(humidity, 6) over (order by date) as target_humidity_day_6,
        lead(pressure_surface, 6) over (order by date) as target_pressure_surface_day_6,
        lead(is_raining, 6) over (order by date) as target_is_raining_day_6,
        
        lead(temp_mean, 7) over (order by date) as target_temp_mean_day_7,
        lead(temp_max, 7) over (order by date) as target_temp_max_day_7,
        lead(temp_min, 7) over (order by date) as target_temp_min_day_7,
        lead(wind_speed, 7) over (order by date) as target_wind_speed_day_7,
        lead(humidity, 7) over (order by date) as target_humidity_day_7,
        lead(pressure_surface, 7) over (order by date) as target_pressure_surface_day_7,
        lead(is_raining, 7) over (order by date) as target_is_raining_day_7,
        
    from cleaned
)

select * from features
where date < current_date -- Filter out future empty rows if any
  );
