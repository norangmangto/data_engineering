
  
    
    

    create  table
      "transport"."main"."int_network_risk__dbt_tmp"
  
    as (
      with stops as (
    select * from "transport"."main"."stg_gtfs_stops"
),

accidents as (
    select * from "transport"."main"."stg_accidents"
),

stop_risk as (
    select
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        -- Count accidents within approx 500m (0.005 degrees roughly)
        count(a.category) as accident_count,
        -- Weighted score: Category 1 (Fatal) = 5 pts, Cat 2 (Serious) = 3 pts, Cat 3 (Minor) = 1 pt
        sum(
            case
                when a.category = 1 then 5
                when a.category = 2 then 3
                else 1
            end
        ) as risk_score
    from stops s
    left join accidents a
    on abs(s.stop_lat - a.lat) < 0.005
    and abs(s.stop_lon - a.lon) < 0.005
    group by 1, 2, 3, 4
)

select * from stop_risk
    );
  
  