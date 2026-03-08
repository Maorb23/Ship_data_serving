
-- SQL query to fetch the most recent position for each ship from the 
-- target database.

WITH ranked AS (
    SELECT
        ship_name,
        time_ts,
        lat,
        lon,
        sog,
        cog,
        ROW_NUMBER() OVER (PARTITION BY ship_name ORDER BY time_ts DESC) AS rn
    FROM target_db.silver.SHIP_POSITIONS
)
SELECT
    ship_name,
    epoch(time_ts) AS time,
    lat,
    lon,
    sog,
    cog
FROM ranked
WHERE rn = 1;
