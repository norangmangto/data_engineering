
  
    
    

    create  table
      "transport"."main"."stg_gtfs_stops__dbt_tmp"
  
    as (
      with source as (
    select * from read_csv_auto('../data/raw/gtfs/stops.txt')
)
select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon
from source
    );
  
  