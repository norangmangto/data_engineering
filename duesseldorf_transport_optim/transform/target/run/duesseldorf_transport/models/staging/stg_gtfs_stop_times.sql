
  
    
    

    create  table
      "transport"."main"."stg_gtfs_stop_times__dbt_tmp"
  
    as (
      with source as (
    select * from read_csv_auto('../data/raw/gtfs/stop_times.txt')
)
select * from source
    );
  
  