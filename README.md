# Ship Positions Pipeline

Production-grade ELT pipeline for vessel sensor data with safe incremental processing and analytical enrichments.

## What this implementation delivers

- Incremental loads with idempotent anti-join on composite key `(ship_name, time)`.
- SQL-driven DB interactions: all executable queries are in `elt_utils/sql/`.
- Reusable `DBops` wrapper that loads query files and executes them against DuckDB.
- Mixed DQ policy:
  - **Critical failures** (missing/invalid `ship_name`, `time`, `lat`, `lon`, plus sequential anomalies) are rejected.
  - **Non-critical failures** (`sog`, `cog`) are nulled and retained.
- Business enrichments:
  - `rot` with 360° wrap-around
  - `acceleration` (`Δsog`)
  - `distance_traveled` via Haversine (nautical miles)
- Transactional publish into `silver.SHIP_POSITIONS` with reject capture table.

## Project flow

1. **Config** (`elt_utils/transform/config.py`)
   - Loads and validates `src/config/config.yaml`.
   - Resolves absolute paths and validates SQL file inventory.

2. **Delta** (`elt_utils/transform/delta.py` + `elt_utils/sql/extract_delta.sql`)
   - Deduplicates source keys (`ship_name`, `time`) with deterministic quality ranking.
   - Extracts only rows not already present in target.

3. **DQA** (`elt_utils/transform/dqa.py`)
   - Applies mixed policy.
  - Computes per-ship sequential deltas (`prev_time`, `time_diff_sec`, `sog_diff`).
  - Rejects rows with non-increasing time or extreme `sog` jumps.
   - Sends critical rows to reject stream with reason codes.

4. **BL** (`src/bl/bl.py`)
   - Pulls per-ship target lookback (`elt_utils/sql/fetch_target_lookback.sql`).
   - Computes `rot`, `acceleration`, `distance_traveled` with continuity across runs.

5. **Publish** (`elt_utils/transform/publish.py`)
   - Creates target/reject tables if missing.
   - Publishes in transaction:
     - reject rows → `silver.SHIP_POSITIONS_REJECTS`
     - accepted rows → `silver.SHIP_POSITIONS`

## Target schema

### `silver.SHIP_POSITIONS`

- `ship_name`
- `time_ts`
- `lat`
- `lon`
- `sog`
- `cog`
- `rot`
- `acceleration`
- `distance_traveled`
- `elt_published_at`

### `silver.SHIP_POSITIONS_REJECTS`

- `ship_name`
- `raw_time`
- `lat`
- `lon`
- `sog`
- `cog`
- `dq_reason`
- `elt_rejected_at`

## How to run

From project root:

```bash
python src/run_me.py --source-db data/raw/ship_positions.db --log-level WARNING
python src/run_me.py --source-db data/raw/ship_positions_incremental.db --log-level WARNING
```

Windows (configured environment example):

```powershell
C:/Users/maorb/anaconda3/envs/prez/python.exe src/run_me.py --source-db data/raw/ship_positions.db
C:/Users/maorb/anaconda3/envs/prez/python.exe src/run_me.py --source-db data/raw/ship_positions_incremental.db
```

Output is a JSON summary (`delta_rows`, `clean_rows`, `rejected_rows`, `published_rows`).

## Configuration

Main config file: `src/config/config.yaml`

- DB paths (`source_db`, `target_db`)
- schema/table names
- DQ thresholds (including `max_sog_diff` for sequential `sog` jump rejection)
- query file mapping

## Data quality decisions

- **Reject (critical):**
  - `ship_name` missing/blank
  - `time` missing or non-positive
  - `lat` missing or out of range `[-90, 90]`
  - `lon` missing or out of range `[-180, 180]`
  - Non-increasing time per ship (`time_diff_sec <= 0`) → `time_not_increasing`
  - Extreme speed jump per ship (`abs(Δsog) > max_sog_diff`) → `sog_extreme_diff`

- **Keep row, null field (non-critical):**
  - `sog` missing/out of configured range
  - `cog` missing/out of configured range

## Incremental strategy and safety

- New rows identified by anti-join against target key `(ship_name, time_ts)` where `time_ts = to_timestamp(time)`.
- Re-running against same source is idempotent (no duplicate publishes).
- Publish uses explicit transaction (`BEGIN/COMMIT/ROLLBACK`).
- BL continuity uses last published row per ship as lookback baseline.

## Tests

Run unit tests:

```bash
pytest tests/unit -q
```

Included tests cover:

- DQ mixed policy behavior
- Sequential DQ anomaly rejection (`time_not_increasing`, `sog_extreme_diff`)
- ROT wrap-around + Haversine enrichment
- Delta dedup + anti-join incremental extraction

Run the full-load + incremental integration test with live stage logs:

```bash
pytest tests/unit/test_pipeline_integration.py -q -o log_cli=true --log-cli-level=INFO
```

## Assumptions and trade-offs

- Composite business key is `(ship_name, time)`.
- `sog` upper bound is configurable (`100` by default) for corruption filtering.
- `max_sog_diff` is configurable (`40` by default) for abrupt per-ship `sog` jump filtering.
- Sequential DQ checks compare each row to the previous row for the same ship within the current delta batch.
- All DB queries are externalized to SQL files for auditability and reuse.
- Timestamp matching relies on deterministic DuckDB `to_timestamp` conversion.
