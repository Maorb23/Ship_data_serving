
-- SQL query to insert rejected records into the silver layer rejects table.


INSERT INTO target_db.silver.SHIP_POSITIONS_REJECTS (
    ship_name,
    raw_time,
    lat,
    lon,
    sog,
    cog,
    dq_reason,
    elt_rejected_at
)
SELECT
    ship_name,
    time AS raw_time,
    lat,
    lon,
    sog,
    cog,
    dq_reason,
    CURRENT_TIMESTAMP
FROM rejects_df;
