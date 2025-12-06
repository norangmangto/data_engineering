with source as (
    select * from read_csv_auto('../data/raw/accidents/Unfallorte.csv')
)
select
    UJAHR as year,
    UMONAT as month,
    USTUNDE as hour,
    WOCHENTAG as weekday,
    UKATEGORIE as category,
    XGCSWGS84 as lon,
    YGCSWGS84 as lat
from source
where lon is not null and lat is not null