-- SQL query to insert new ship position records into the target silver table, 
-- avoiding duplicates.

WITH src AS (
    SELECT
        ship_name,
        time,
        lat,
        lon,
        sog,
        cog,
        rot,
        acceleration,
        distance_traveled,
        ROW_NUMBER() OVER (PARTITION BY ship_name, time ORDER BY time DESC) AS rn
    FROM publish_df
)
INSERT INTO target_db.silver.SHIP_POSITIONS (
    ship_name,
    time_ts,
    lat,
    lon,
    sog,
    cog,
    rot,
    acceleration,
    distance_traveled,
    elt_published_at
)
SELECT
    s.ship_name,
    CAST(to_timestamp(s.time) AS TIMESTAMP) AS time_ts,
    s.lat,
    s.lon,
    s.sog,
    s.cog,
    s.rot,
    s.acceleration,
    s.distance_traveled,
    CURRENT_TIMESTAMP AS elt_published_at
FROM src s
LEFT JOIN target_db.silver.SHIP_POSITIONS t
    ON t.ship_name = s.ship_name
   AND t.time_ts = CAST(to_timestamp(s.time) AS TIMESTAMP)
WHERE s.rn = 1
  AND t.ship_name IS NULL;
