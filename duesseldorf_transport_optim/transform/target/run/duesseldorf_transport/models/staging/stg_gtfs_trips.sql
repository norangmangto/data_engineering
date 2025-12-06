
  
    
    

    create  table
      "transport"."main"."stg_gtfs_trips__dbt_tmp"
  
    as (
      with source as (
    select * from read_csv_auto('../data/raw/gtfs/trips.txt')
)
select * from source
    );
  
  