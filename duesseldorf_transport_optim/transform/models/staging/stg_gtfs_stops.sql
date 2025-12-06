with source as (
    select * from read_csv_auto('../data/raw/gtfs/stops.txt')
)
select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from source
