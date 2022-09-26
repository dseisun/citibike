begin transaction;
--Insert new stations
insert into prod.stations as stns (
    citi_station_id,
    station_name,
    lat,
    long,
    first_trip_at,
    last_trip_at,
    created_at
)

select 
    stg.citi_station_id,
    stg.station_name,
    stg.lat,
    stg.long,
    stg.first_trip_at,
    stg.last_trip_at,
    stg.created_at
from staging.stations stg
on conflict (citi_station_id) do nothing;

-- Adjust the first seen and last seen times to existing stations if necessary
with joined as (
select 
    stns.citi_station_id,
    case when stns.first_trip_at < stg.first_trip_at then stns.first_trip_at else stg.first_trip_at end as first_trip_at,
    case when stns.last_trip_at > stg.last_trip_at then stns.last_trip_at else stg.last_trip_at end as last_trip_at
from staging.stations stg
left join prod.stations stns on stg.citi_station_id = stns.citi_station_id)

update prod.stations t
set first_trip_at = joined.first_trip_at, last_trip_at = joined.last_trip_at
from joined
where t.citi_station_id = joined.citi_station_id;

-- Log the loaded stations
insert into prod.stations_log (
    citi_station_id,
    station_name,
    lat,
    long,
    first_trip_at,
    last_trip_at,
    filename
)
select
    stg.citi_station_id,
    stg.station_name,
    stg.lat,
    stg.long,
    stg.first_trip_at,
    stg.last_trip_at,
    stg.filename
from staging.stations stg;

--Truncate the staging table
truncate table staging.stations;
end transaction;