-- This model reads from our raw table and performs basic cleaning.

SELECT 
    -- IDs
    trip_id,
    route_id,
    vehicle_id,
    stop_id,
    
    -- Location
    latitude,
    longitude,
    
    -- Status
    current_status,
    
    -- Timestamps
    -- The raw timestamp is in 'epoch' seconds, let's convert it to a real timestamp
    FROM_UNIXTIME(timestamp) AS event_timestamp,
    loaded_at AS loaded_at_warehouse

FROM 
    -- Select directly from our raw table in our database
    lirr_db.raw_lirr_updates