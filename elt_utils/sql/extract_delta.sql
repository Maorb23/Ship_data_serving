
-- SQL script for extracting new ship position records from the raw source table and 
-- identifying any duplicates based on ship_name and time. The script selects the 
-- most complete record for each ship_name and time combination, and then filters 
-- out any records that already exist in the target SHIP_POSITIONS table.


WITH source_ranked AS (
    SELECT
        ship_name,
        time,
        lat,
        lon,
        sog,
        cog,
        ROW_NUMBER() OVER (
            PARTITION BY ship_name, time
            ORDER BY
                CASE WHEN lat IS NULL THEN 0 ELSE 1 END DESC,
                CASE WHEN lon IS NULL THEN 0 ELSE 1 END DESC,
                CASE WHEN sog IS NULL THEN 0 ELSE 1 END DESC,
                CASE WHEN cog IS NULL THEN 0 ELSE 1 END DESC
        ) AS candidate_rank
    FROM raw.ship_positions
),
source_dedup AS (
    SELECT ship_name, time, lat, lon, sog, cog
    FROM source_ranked
    WHERE candidate_rank = 1
),
new_rows AS (
    SELECT s.*
    FROM source_dedup s
    LEFT JOIN target_db.silver.SHIP_POSITIONS t
        ON t.ship_name = s.ship_name
       AND t.time_ts = CAST(to_timestamp(s.time) AS TIMESTAMP)
    WHERE t.ship_name IS NULL
)
SELECT ship_name, time, lat, lon, sog, cog
FROM new_rows
ORDER BY ship_name, time;
