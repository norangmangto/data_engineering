with source as (
    select * from read_csv_auto('../data/raw/gtfs/stop_times.txt')
)
select * from source
