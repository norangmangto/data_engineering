with source as (
    select * from read_csv_auto('../data/raw/gtfs/trips.txt')
)
select * from source