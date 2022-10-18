DROP TABLE IF EXISTS prod.trips_cleaned;
create table prod.trips_cleaned as 
 SELECT 
 	trips.started_at,
    trips.ended_at,
    trips.start_station_id,
    trips.end_station_id,
    trips.membership_status,
        CASE
            WHEN trips.rideable_type::text = 'docked_bike'::text THEN 'classic_bike'::character varying
            ELSE trips.rideable_type
        END AS rideable_type,
    trips.filename
   FROM prod.trips
  WHERE (trips.ended_at - trips.started_at) >= '00:00:00'::interval minute AND (trips.ended_at - trips.started_at) <= '02:00:00'::interval minute;
-- Create indexes on the table for faster joining to stations
CREATE INDEX trips_start_station_id ON prod.trips_cleaned (start_station_id ASC NULLS LAST);
CREATE INDEX trips_end_station_id ON prod.trips_cleaned (end_station_id ASC NULLS LAST);
