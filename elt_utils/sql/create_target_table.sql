-- SQL script to create the target table in the silver layer of the ELT pipeline.


CREATE SCHEMA IF NOT EXISTS target_db.silver;

CREATE TABLE IF NOT EXISTS target_db.silver.SHIP_POSITIONS (
    ship_name VARCHAR NOT NULL,
    time_ts TIMESTAMP NOT NULL,
    lat DOUBLE,
    lon DOUBLE,
    sog DOUBLE,
    cog DOUBLE,
    rot DOUBLE,
    acceleration DOUBLE,
    distance_traveled DOUBLE,
    elt_published_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ship_positions_key
ON target_db.silver.SHIP_POSITIONS (ship_name, time_ts);
