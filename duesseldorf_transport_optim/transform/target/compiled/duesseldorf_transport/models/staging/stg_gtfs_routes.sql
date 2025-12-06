with source as (
    select * from read_csv_auto('../data/raw/gtfs/routes.txt')
)
select * from source