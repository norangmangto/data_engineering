
  
    
    

    create  table
      "transport"."main"."stg_gtfs_routes__dbt_tmp"
  
    as (
      with source as (
    select * from read_csv_auto('../data/raw/gtfs/routes.txt')
)
select * from source
    );
  
  