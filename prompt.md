# `Data Engineer Assignment: Ship Positions Pipeline`

## `Task Description:`

Design a **production-grade data pipeline** that processes ship sensor data and builds an analytical table.

You are provided with a **pipeline skeleton** (folders, class signatures, and method stubs). It illustrates the intended architecture, but you are free to modify or extend it as needed.

Approach this as if you were building a real production pipeline:

- What makes it robust?
- How do you prevent data corruption?
- How do you guarantee safe incremental processing?
- How do you make it configurable and testable?

The pipeline need to support **incremental loads** 

## `Task Guidelines:`

### `1. Project Skeleton`:

#### `1.a. ship_positions/data:`
- data/raw/ship_positions.db — The source table
- data/raw/ship_positions_incremental.db — an increnemtal table used to test incremental load. It contains a continuation of the data in data/raw/ship_positions.db and should be used to verify that your pipeline correctly processes only new records when run against an updated source.

#### `1.b. ship_positions/elt_utils/` — Reusable ELT Framework

This layer should be generic and reusable across pipeline jobs.

You must implement the logic in each module:

- elt_utils/db/duckdb.py — DuckDB wrapper to work with the DB
- elt_utils/transform/config.py — Configuration loader
- elt_utils/transform/delta.py — Incremental extraction logic
- elt_utils/transform/dqa.py — To handle Data quality issues
- elt_utils/transform/publish.py — Target table publishing logic


#### `1.c. ship_positions/src/` — Ship Positions Pipeline Job

This is the job-specific implementation built on your framework.

- src/run_me.py — Entry point
- src/pipeline/pipe.py — Orchestrates all pipeline stages
- src/bl/bl.py — Business logic and derived features

### `2.Source Dataset`

#### `2.a ship_positions/data/raw:` raw.ship_positions

The `raw.ship_positions` table contains voyage tracking data for **two vessels**.
Each ship records its geographic position and navigation metrics throughout its journey.

Each row represents a single **sensor snapshot**, including:

- The ship name (vessel identifier)
- The record timestamp
- The ship’s geographic location (latitude/longitude)
- Its speed over ground (SOG)
- Its course over ground (COG)

Over a full voyage, this results in **thousands of records per ship**, allowing you to reconstruct:

- The vessel’s route
- Its speed patterns
- Direction changes
- Behavioral patterns over time

This dataset represents the **raw sensor feed** from the ships.

In a real-world scenario, this table would be **continuously updated** as new sensor readings are transmitted from the vessels. Your pipeline should therefore assume that new records are constantly appended and must be processed incrementally.

####  `2.b. Table Schema:`

| Column | Type | Notes |
|---|---|---|
| **ship_name** | VARCHAR | Vessel identifier |
| **time** | DOUBLE | Epoch timestamp in seconds (float) |
| **lat** | FLOAT | Latitude in decimal degrees, −90 to 90 |
| **lon** | FLOAT | Longitude in decimal degrees, −180 to 180 |
| **sog** | FLOAT | Speed over ground in knots |
| **cog** | FLOAT | Course over ground, 0–360° |

`**Note:**` **The data is not clean.** Part of your job is to identify what is wrong with it and decide how each issue should be handled.

---

### `3. Target Table`

#### `silver.SHIP_POSITIONS`:

The final table should include the following schema:

| Column | Description                                                                                   | Notes |
|--------|-----------------------------------------------------------------------------------------------|-------|
| **ship_name** | Vessel identifier                                                                             | |
| **time_ts** | recorded time (in timestamp format)                                                           | |
| **lat** | Cleaned latitude in decimal degrees                                                           | |
| **lon** | Cleaned longitude in decimal degrees                                                          | |
| **sog** | Speed over ground in knots (cleaned)                                                          | |
| **cog** | Course over ground in degrees (cleaned)                                                       | |
| **rot** | Rate of Turn — change in COG between consecutive records for the same vessel (handle 360° wrap-around)                 | Calculated field |
| **acceleration** | Change in SOG between consecutive records for the same vessel                                 | Calculated field |
| **distance_traveled** | Distance from the previous position for the same vessel in nautical miles (Haversine formula) | Calculated field |
| **elt_published_at** | Timestamp set at publish time                                                                 | Operational Field |


### `4. Overall Pipeline:`

- Reads from raw.ship_positions incrementally (processing only new records on each run)
- Handles all data quality issues in the source data
- Applies all business logic and enrichments
- Publishes clean, validated records into silver.SHIP_POSITIONS.db

The code must run successfully twice: On data/raw/ship_positions.db (full load) And on data/raw/ship_positions_incremental.db (incremental run), i.e. with new rows identification.




### `5. Pipeline Documentation:`

A README.md in the project root explaining:

- How to run the pipeline (full load and incremental)
- What each stage does and why
- How data quality issues are handled and what decisions were made
- Any assumptions or trade-offs


