
-- SQL script to create the SHIP_POSITIONS_REJECTS table in the target database.


CREATE SCHEMA IF NOT EXISTS target_db.silver;

CREATE TABLE IF NOT EXISTS target_db.silver.SHIP_POSITIONS_REJECTS (
    ship_name VARCHAR, 
    raw_time DOUBLE,
    lat DOUBLE,
    lon DOUBLE,
    sog DOUBLE,
    cog DOUBLE,
    dq_reason VARCHAR,
    elt_rejected_at TIMESTAMP NOT NULL
);
