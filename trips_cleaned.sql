DROP TABLE IF EXISTS prod.trips_cleaned;
create table prod.trips_cleaned as 
 SELECT 
 	trips.started_at,
    trips.ended_at,
    trips.start_station_name,
    trips.end_station_name,
    case 
        when trips.membership_status = 'Subscriber' then 'member' 
        when trips.membership_status = 'Customer' then 'casual'
    END as membership_status,
        CASE
            WHEN trips.rideable_type = 'docked_bike' THEN 'classic_bike'
            when trips.rideable_type is null then 'classic_bike'
            ELSE rideable_type
        END AS rideable_type,
    trips.filename
   FROM prod.trips
  WHERE (trips.ended_at - trips.started_at) >= '00:00:00'::interval minute AND (trips.ended_at - trips.started_at) <= '02:00:00'::interval minute;
-- Create indexes on the table for faster joining to stations
CREATE INDEX trips_start_station_name ON prod.trips_cleaned (start_station_name ASC NULLS LAST);
CREATE INDEX trips_end_station_name ON prod.trips_cleaned (end_station_name ASC NULLS LAST);
