with source as (
    select * from read_csv_auto('/Users/norangmangto/works/data_engineering/dwd_weather_prediction/data/raw/dwd_daily_01078.csv')
),

renamed as (
    select
        station_id,
        date::date as date,
        parameter,
        value,
        quality
    from source
)

select * from renamed
