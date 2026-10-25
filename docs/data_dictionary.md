# Data Dictionary: Loop IQ Open-Meteo Medallion Lakehouse

This document defines the objective, grain, update semantics, **exact field origin/derivation**, and examples for every table across our **Operational (`ops`)**, **Bronze (`bronze`)**, **Silver (`silver`)**, and **Gold (`gold`)** layers.

---

## Origin / Source Legend
Every column specifies one of five origins:
1. **`[API Response]`**: Extracted directly from the Open-Meteo `/v1/forecast` JSON response (exact JSON path provided).
2. **`[Seed Config: locations.json]`**: Loaded from our local facility registry file (`config/locations.json`), which supplies business context (`location_id`, `location_name`, `state_code`, `timezone_name`, `climate_risk_tier`).
3. **`[.env Config]`**: Controlled by runtime environment variables in `.env`.
4. **`[Pipeline Derived]`**: Computed by our Python/PySpark transformation logic using a deterministic formula, hash, or lookup table.
5. **`[System Metadata]`**: Generated at runtime by the execution engine (UUIDs, UTC timestamps, HTTP status codes, file paths).

---

## 1. Operational & Governance Layer (`ops`)

### 1.1 Table: `ops.api_call_weight_ledger`
* **Objective:** Tracks Open-Meteo API quota consumption and weighted call cost **strictly after each HTTP call has received a response from the Open-Meteo API server**. Logs all server HTTP responses (`200`, `4xx`, `5xx`), counting `consumed_call_weight > 0` on `HTTP 200 OK` (`is_quota_consumed = true`) and `0.0` on non-200 status codes. Pre-flight failures or offline cache reads never write to this table.
* **Storage Format & Update Strategy:** Delta Lake (`APPEND` only, post-HTTP response).
* **Grain:** One row per completed outbound HTTP request to the Open-Meteo API.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `call_id` | `STRING` | `NO` | `[System Metadata]` | Generated via Python `uuid.uuid4()` immediately after the HTTP response is received from Open-Meteo. | `"c9a4f812-3b7d-4e91-8a1c-5f6e2d1b904a"` |
| `pipeline_run_id` | `STRING` | `NO` | `[System Metadata]` | Generated at CLI startup (`run-<UTC_YYYYMMDD_HHMMSS>-<short_uuid>`) to group all steps in a single pipeline run. | `"run-20261023-070512-8f3a"` |
| `endpoint_url` | `STRING` | `NO` | `[.env Config]` | Read from `.env` `OPEN_METEO_BASE_URL` (with any sensitive `apikey` query parameter masked). | `"https://api.open-meteo.com/v1/forecast"` |
| `http_status_code` | `INT` | `NO` | `[System Metadata]` | Extracted from the HTTP response `response.status_code` returned by the Open-Meteo server. | `200` |
| `is_quota_consumed` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | Derived as `true` when `http_status_code == 200`, else `false`. | `true` |
| `num_locations` | `INT` | `NO` | `[Seed Config: locations.json]` | Count of coordinate pairs batched into the comma-separated `latitude` and `longitude` query parameters ($N_{\text{locations}}$). | `8` |
| `num_days` | `INT` | `NO` | `[.env Config]` | Sum of `.env` `WEATHER_PAST_DAYS + WEATHER_FORECAST_DAYS` requested in the call ($N_{\text{days}}$). | `14` |
| `num_variables` | `INT` | `NO` | `[.env Config]` | Total count of distinct weather variables requested across `WEATHER_MINUTELY_15_VARS`, `WEATHER_HOURLY_VARS`, and `WEATHER_DAILY_VARS` ($N_{\text{variables}}$). | `21` |
| `consumed_call_weight` | `DOUBLE` | `NO` | `[Pipeline Derived]` | If `http_status_code == 200`, calculated using Open-Meteo's weight formula with a baseline floor of `1.0`: `num_locations * max(1.0, num_days / 14.0) * max(1.0, num_variables / 10.0)`. Else `0.0`. | `16.8` |
| `rolling_1m_weight` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Sum of `consumed_call_weight` over the trailing 60 seconds including current call (limit: `600.0`). | `16.8` |
| `rolling_1h_weight` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Sum of `consumed_call_weight` over the trailing 60 minutes including current call (limit: `5000.0`). | `33.6` |
| `rolling_24h_weight` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Sum of `consumed_call_weight` over the trailing 24 hours including current call (limit: `10000.0`). | `100.8` |
| `daily_quota_remaining_pct` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Calculated as `round(max(0.0, (1.0 - (rolling_24h_weight / API_RATE_LIMIT_PER_DAY)) * 100), 2)`. | `98.99` |
| `response_latency_ms` | `DOUBLE` | `NO` | `[System Metadata]` | Measured elapsed time of the HTTP GET call in milliseconds. | `342.5` |
| `landing_file_path` | `STRING` | `YES` | `[System Metadata]` | Relative path in the Landing Zone where the raw HTTP response JSON was saved (`NULL` if non-200). | `"data/landing/raw_json/ingestion_date=2026-10-23/batch_c9a4f812.json"` |
| `called_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Exact UTC timestamp recorded upon receiving the HTTP response. | `2026-10-23 00:05:14.210 UTC` |
| `call_date` | `DATE` | `NO` | `[Pipeline Derived]` | Derived as `CAST(called_at_utc AS DATE)`. | `2026-10-23` |

---

### 1.2 Table / Log: `ops.pipeline_step_execution_log` (and Local `logs/pipeline_steps_audit.jsonl`)
* **Objective:** Records `STARTED`, `SUCCESS`, `FAILED`, and `SKIPPED_ON_REPAIR` execution telemetry for every discrete step of the pipeline. Enables **automated repair runs** (`cli.py repair --run-id <id>`) so a failed run resumes at the exact failed step without re-calling the Open-Meteo API.
* **Storage Format & Update Strategy:** Dual-written to local JSONL file (`logs/pipeline_steps_audit.jsonl`) for instant crash survival AND Delta table `ops.pipeline_step_execution_log`.
* **Grain:** One row per `(pipeline_run_id, step_name, attempt_number)`.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `step_log_id` | `STRING` | `NO` | `[System Metadata]` | UUID generated when the step state transition is logged. | `"e11f22a3-44b5-66c7-88d9-aabbccddeeff"` |
| `pipeline_run_id` | `STRING` | `NO` | `[System Metadata]` | Unique identifier for the pipeline run. | `"run-20261023-070512-8f3a"` |
| `step_sequence_order` | `INT` | `NO` | `[Pipeline Derived]` | Deterministic integer order of the step (`1` to `7`). | `4` |
| `step_name` | `STRING` | `NO` | `[Pipeline Derived]` | Canonical name of the pipeline stage being executed. | `"step_4_landing_to_bronze_daily"` |
| `step_status` | `STRING` | `NO` | `[System Metadata]` | Execution outcome: `"STARTED"`, `"SUCCESS"`, `"FAILED"`, or `"SKIPPED_ON_REPAIR"`. | `"SUCCESS"` |
| `input_artifact_ref` | `STRING` | `YES` | `[System Metadata]` | Checkpoint pointer to the input consumed by this step (e.g., `landing_file_path` or `pipeline_run_id`). | `"data/landing/raw_json/ingestion_date=2026-10-23/batch_c9a4f812.json"` |
| `rows_processed` | `LONG` | `YES` | `[System Metadata]` | Count of valid records written/merged into the target table during this step. | `8` |
| `rows_quarantined` | `LONG` | `YES` | `[System Metadata]` | Count of records routed to `silver.quarantine_weather_records` during this step. | `0` |
| `duration_seconds` | `DOUBLE` | `YES` | `[System Metadata]` | Wall-clock execution time of the step in seconds. | `1.42` |
| `error_type` | `STRING` | `YES` | `[System Metadata]` | Python exception class name if `step_status == "FAILED"`, else `NULL`. | `NULL` |
| `error_message` | `STRING` | `YES` | `[System Metadata]` | Full exception message and traceback snippet if `step_status == "FAILED"`. | `NULL` |
| `logged_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when the status event was written. | `2026-10-23 00:05:18.900 UTC` |

---

## 2. Bronze Layer (`bronze`) — 3 Granular Lineage Tables

When Open-Meteo responds, we request each facility's local IANA timezone (`timezone=Australia/Sydney,Australia/Melbourne,...`) so daily summaries and local dates align to midnight-to-midnight at each facility. Both `utc_offset_seconds` and `timezone` from the top-level API response are captured in all 3 Bronze tables so Silver can compute true UTC timestamps accurately.

### 2.1 Table: `bronze.weather_minutely_15`
* **Objective:** Preserves the raw 15-minute interval JSON arrays (`minutely_15` and `minutely_15_units`) per location alongside seed facility metadata, API grid-cell coordinates, and `utc_offset_seconds`.
* **Storage Format & Update Strategy:** Delta Lake (`APPEND` only, partitioned by `ingestion_date`).
* **Grain:** One row per `(ingestion_id, location_id)` per API call.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Unique UUID generated for this Bronze table record (`uuid.uuid4()`). | `"m15-71e9204-882a-4c11-90ef-123456789abc"` |
| `call_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to `ops.api_call_weight_ledger.call_id`. | `"c9a4f812-3b7d-4e91-8a1c-5f6e2d1b904a"` |
| `pipeline_run_id` | `STRING` | `NO` | `[System Metadata]` | Identifier of the pipeline run that ingested this record. | `"run-20261023-070512-8f3a"` |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].location_id`) and matched positionally to the $i$-th element of the batched API response array. | `"AU-NSW-SYD-01"` |
| `location_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].location_name`). | `"Sydney Metro Care Hub"` |
| `state_code` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].state_code`). | `"NSW"` |
| `climate_risk_tier` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].climate_risk_tier`). | `"STANDARD"` |
| `timezone_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Local IANA timezone from `config/locations.json` (`locations[i].timezone_name`), confirmed by `response[i].timezone`. | `"Australia/Sydney"` |
| `utc_offset_seconds` | `INT` | `NO` | `[API Response]` | Extracted from top-level `response[i].utc_offset_seconds` (e.g., `39600` for AEDT `UTC+11`, `34200` for Darwin `UTC+9:30`). Used in Silver to convert local timestamps to true UTC. | `39600` |
| `requested_latitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Exact WGS84 latitude sent in the API request (`locations[i].latitude`). | `-33.8688` |
| `requested_longitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Exact WGS84 longitude sent in the API request (`locations[i].longitude`). | `151.2093` |
| `model_grid_latitude` | `DOUBLE` | `NO` | `[API Response]` | Extracted from `response[i].latitude` (snapped weather model grid coordinate). | `-33.85` |
| `model_grid_longitude` | `DOUBLE` | `NO` | `[API Response]` | Extracted from `response[i].longitude`. | `151.20` |
| `elevation_m` | `DOUBLE` | `YES` | `[API Response]` | Extracted from `response[i].elevation`. | `42.0` |
| `minutely_15_units_json` | `STRING` | `NO` | `[API Response]` | Serialized JSON string of `response[i].minutely_15_units`. | `'{"time":"iso8601","temperature_2m":"°C",...}'` |
| `minutely_15_payload_json` | `STRING` | `NO` | `[API Response]` | Serialized JSON string of `response[i].minutely_15` containing parallel arrays in facility local time. | `'{"time":["2026-10-16T00:00","2026-10-16T00:15",...],"temperature_2m":[17.2,17.1,...]}'` |
| `landing_file_path` | `STRING` | `NO` | `[System Metadata]` | Path to the raw HTTP response JSON file in the Landing Zone. | `"data/landing/raw_json/ingestion_date=2026-10-23/batch_c9a4f812.json"` |
| `payload_sha256` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of `minutely_15_payload_json`. | `"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"` |
| `ingested_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when appended to Bronze. | `2026-10-23 00:05:15.100 UTC` |
| `ingestion_date` | `DATE` | `NO` | `[Pipeline Derived]` | Derived as `CAST(ingested_at_utc AS DATE)` (partition key). | `2026-10-23` |

---

### 2.2 Table: `bronze.weather_hourly`
* **Objective:** Preserves the raw hourly interval JSON arrays (`hourly` and `hourly_units`) per location alongside seed facility metadata, API grid coordinates, and `utc_offset_seconds`.
* **Storage Format & Update Strategy:** Delta Lake (`APPEND` only, partitioned by `ingestion_date`).
* **Grain:** One row per `(ingestion_id, location_id)` per API call.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Unique UUID generated for this Bronze hourly record. | `"hr-b71e9204-882a-4c11-90ef-123456789abc"` |
| `call_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to `ops.api_call_weight_ledger.call_id`. | `"c9a4f812-3b7d-4e91-8a1c-5f6e2d1b904a"` |
| `pipeline_run_id` | `STRING` | `NO` | `[System Metadata]` | Identifier of the pipeline run that ingested this record. | `"run-20261023-070512-8f3a"` |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].location_id`). | `"AU-NSW-SYD-01"` |
| `location_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].location_name`). | `"Sydney Metro Care Hub"` |
| `state_code` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].state_code`). | `"NSW"` |
| `climate_risk_tier` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].climate_risk_tier`). | `"STANDARD"` |
| `timezone_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Local IANA timezone from `config/locations.json` (`locations[i].timezone_name`). | `"Australia/Sydney"` |
| `utc_offset_seconds` | `INT` | `NO` | `[API Response]` | Extracted from `response[i].utc_offset_seconds`. | `39600` |
| `requested_latitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Configured facility latitude (`locations[i].latitude`). | `-33.8688` |
| `requested_longitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Configured facility longitude (`locations[i].longitude`). | `151.2093` |
| `model_grid_latitude` | `DOUBLE` | `NO` | `[API Response]` | Extracted from Open-Meteo `response[i].latitude`. | `-33.85` |
| `model_grid_longitude` | `DOUBLE` | `NO` | `[API Response]` | Extracted from Open-Meteo `response[i].longitude`. | `151.20` |
| `elevation_m` | `DOUBLE` | `YES` | `[API Response]` | Extracted from Open-Meteo `response[i].elevation`. | `42.0` |
| `hourly_units_json` | `STRING` | `NO` | `[API Response]` | Serialized JSON string of `response[i].hourly_units`. | `'{"time":"iso8601","temperature_2m":"°C",...}'` |
| `hourly_payload_json` | `STRING` | `NO` | `[API Response]` | Serialized JSON string of `response[i].hourly` containing the parallel hourly arrays in facility local time. | `'{"time":["2026-10-16T00:00",...],"temperature_2m":[17.2,...]}'` |
| `landing_file_path` | `STRING` | `NO` | `[System Metadata]` | Path to the raw HTTP response JSON file in the Landing Zone. | `"data/landing/raw_json/ingestion_date=2026-10-23/batch_c9a4f812.json"` |
| `payload_sha256` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of `hourly_payload_json`. | `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"` |
| `ingested_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when appended to Bronze. | `2026-10-23 00:05:15.250 UTC` |
| `ingestion_date` | `DATE` | `NO` | `[Pipeline Derived]` | Derived as `CAST(ingested_at_utc AS DATE)` (partition key). | `2026-10-23` |

---

### 2.3 Table: `bronze.weather_daily`
* **Objective:** Preserves the raw daily interval JSON arrays (`daily` and `daily_units`) per location alongside seed facility metadata, API grid coordinates, and `utc_offset_seconds`.
* **Storage Format & Update Strategy:** Delta Lake (`APPEND` only, partitioned by `ingestion_date`).
* **Grain:** One row per `(ingestion_id, location_id)` per API call.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Unique UUID generated for this Bronze daily record. | `"dy-a12b34c5-882a-4c11-90ef-123456789abc"` |
| `call_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to `ops.api_call_weight_ledger.call_id`. | `"c9a4f812-3b7d-4e91-8a1c-5f6e2d1b904a"` |
| `pipeline_run_id` | `STRING` | `NO` | `[System Metadata]` | Identifier of the pipeline run that ingested this record. | `"run-20261023-070512-8f3a"` |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].location_id`). | `"AU-NSW-SYD-01"` |
| `location_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].location_name`). | `"Sydney Metro Care Hub"` |
| `state_code` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].state_code`). | `"NSW"` |
| `climate_risk_tier` | `STRING` | `NO` | `[Seed Config: locations.json]` | Read from `config/locations.json` (`locations[i].climate_risk_tier`). | `"STANDARD"` |
| `timezone_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Local IANA timezone from `config/locations.json` (`locations[i].timezone_name`). | `"Australia/Sydney"` |
| `utc_offset_seconds` | `INT` | `NO` | `[API Response]` | Extracted from `response[i].utc_offset_seconds`. | `39600` |
| `requested_latitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Configured facility latitude (`locations[i].latitude`). | `-33.8688` |
| `requested_longitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Configured facility longitude (`locations[i].longitude`). | `151.2093` |
| `model_grid_latitude` | `DOUBLE` | `NO` | `[API Response]` | Extracted from Open-Meteo `response[i].latitude`. | `-33.85` |
| `model_grid_longitude` | `DOUBLE` | `NO` | `[API Response]` | Extracted from Open-Meteo `response[i].longitude`. | `151.20` |
| `elevation_m` | `DOUBLE` | `YES` | `[API Response]` | Extracted from Open-Meteo `response[i].elevation`. | `42.0` |
| `daily_units_json` | `STRING` | `NO` | `[API Response]` | Serialized JSON string of `response[i].daily_units`. | `'{"time":"iso8601","temperature_2m_max":"°C",...}'` |
| `daily_payload_json` | `STRING` | `NO` | `[API Response]` | Serialized JSON string of `response[i].daily` containing parallel daily arrays aggregated over each facility's local 24h calendar day. | `'{"time":["2026-10-16",...],"temperature_2m_max":[24.1,...]}'` |
| `landing_file_path` | `STRING` | `NO` | `[System Metadata]` | Path to the raw HTTP response JSON file in the Landing Zone. | `"data/landing/raw_json/ingestion_date=2026-10-23/batch_c9a4f812.json"` |
| `payload_sha256` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of `daily_payload_json`. | `"4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"` |
| `ingested_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when appended to Bronze. | `2026-10-23 00:05:15.400 UTC` |
| `ingestion_date` | `DATE` | `NO` | `[Pipeline Derived]` | Derived as `CAST(ingested_at_utc AS DATE)` (partition key). | `2026-10-23` |

---

## 3. Silver Layer (`silver`)

### Three-Guard Idempotent `MERGE INTO` Architecture
Every Silver fact table enforces three idempotency guards:
1. **Incremental Read:** Processes only Bronze rows from the current `pipeline_run_id` (or `ingested_at_utc > watermark`) so failed rows are quarantined once.
2. **Source Deduplication:** Deduplicates incoming rows by primary grain ordered by `api_ingested_at_utc DESC` via `ROW_NUMBER() = 1`.
3. **Newer-Only & Value-Changed Update:** `WHEN MATCHED AND src.api_ingested_at_utc >= tgt.api_ingested_at_utc AND src.row_value_hash != tgt.row_value_hash THEN UPDATE SET *` — preventing older replays from clobbering newer forecasts and keeping Delta Change Data Feed (`_change_data`) free of no-op updates.

### 3.1 Table: `silver.dim_location_scd2`
* **Objective:** Slowly Changing Dimension Type 2 (SCD2) table combining business facility attributes from `config/locations.json` with resolved meteorological grid metadata (`model_grid_latitude`, `model_grid_longitude`, `elevation_m`, `utc_offset_seconds`) from Bronze.
* **Storage Format & Update Strategy:** Delta Lake (`MERGE INTO` with SCD Type 2 expiration of old active row and insertion of new active version).
* **Grain:** One row per version of a `location_id` (`is_current = true` filters to the active version).

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `location_sk` | `STRING` | `NO` | `[Pipeline Derived]` | Surrogate primary key computed as `sha2(concat_ws('||', location_id, cast(effective_from_utc as string)), 256)`. | `"8f9d2a1c4b6e7f0a123456789abcdef0..."` |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Natural business key propagated from `bronze.weather_hourly.location_id`. | `"AU-NSW-SYD-01"` |
| `location_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Human-readable facility hub name propagated from `config/locations.json`. | `"Sydney Metro Care Hub"` |
| `state_code` | `STRING` | `NO` | `[Seed Config: locations.json]` | Australian state/territory code propagated from `config/locations.json`. | `"NSW"` |
| `requested_latitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Exact facility latitude configured in `config/locations.json`. | `-33.8688` |
| `requested_longitude` | `DOUBLE` | `NO` | `[Seed Config: locations.json]` | Exact facility longitude configured in `config/locations.json`. | `151.2093` |
| `model_grid_latitude` | `DOUBLE` | `NO` | `[API Response]` | Snapped weather model grid-cell latitude returned by Open-Meteo (`response[i].latitude`). | `-33.85` |
| `model_grid_longitude` | `DOUBLE` | `NO` | `[API Response]` | Snapped weather model grid-cell longitude returned by Open-Meteo (`response[i].longitude`). | `151.20` |
| `grid_snap_distance_km` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Haversine distance in kilometers calculated between `(requested_latitude, requested_longitude)` and `(model_grid_latitude, model_grid_longitude)`. | `2.26` |
| `elevation_m` | `DOUBLE` | `YES` | `[API Response]` | Digital Elevation Model (DEM) height in meters (`response[i].elevation`). | `42.0` |
| `timezone_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Local IANA timezone identifier (`locations[i].timezone_name`). | `"Australia/Sydney"` |
| `utc_offset_seconds` | `INT` | `NO` | `[API Response]` | Local timezone offset in seconds from UTC (`response[i].utc_offset_seconds`). | `39600` |
| `climate_risk_tier` | `STRING` | `NO` | `[Seed Config: locations.json]` | Regional environmental vulnerability tier defined in `config/locations.json`. | `"STANDARD"` |
| `attribute_hash` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of tracked mutable attributes (`location_name`, `state_code`, `model_grid_latitude`, `model_grid_longitude`, `elevation_m`, `climate_risk_tier`). Triggers SCD2 versioning when changed. | `"4a7d1ed414474e4033ac29ccb8653d9b..."` |
| `effective_from_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Set to `ingested_at_utc` of the Bronze batch that introduced this version. | `2026-10-23 00:05:15.250 UTC` |
| `effective_to_utc` | `TIMESTAMP` | `YES` | `[Pipeline Derived]` | `9999-12-31 23:59:59 UTC` while active; updated to the new version's `effective_from_utc` when superseded. | `9999-12-31 23:59:59.000 UTC` |
| `is_current` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | `true` for the latest active version of `location_id`; `false` when closed out. | `true` |

---

### 3.2 Table: `silver.fact_weather_minutely_15`
* **Objective:** Conformed 15-minute grain weather time-series unnested from `bronze.weather_minutely_15`, preserving both the facility's local timestamp/date and true UTC timestamp (`observation_local_time - utc_offset_seconds`).
* **Storage Format & Update Strategy:** **Delta Lake — SCD Type 1 Upsert (3-Guard `MERGE INTO` + Change Data Feed `delta.enableChangeDataFeed = true`)**, partitioned by `observation_local_date`.
  * *Sliding 14-Day Window Lifecycle (`ADR-003`):* Older history ($< T-7$) is untouched and retained permanently; overlapping timestamps ($[T-7, T+5]$) are updated in-place (`WHEN MATCHED`) when newer forecasts or assimilated actuals differ in `row_value_hash` (with prior versions logged in Delta CDF `_change_data`); new horizon timestamps ($T+6$) are appended (`WHEN NOT MATCHED THEN INSERT`).
* **Grain:** One row per `(location_id, observation_local_time)` / `(location_id, observation_time_utc)`.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Propagated from `bronze.weather_minutely_15.location_id`. | `"AU-NSW-SYD-01"` |
| `observation_local_time` | `STRING` | `NO` | `[API Response]` | Local ISO8601 timestamp string `minutely_15.time[k]` in the facility's local timezone. | `"2026-10-23T15:15"` |
| `observation_local_date` | `DATE` | `NO` | `[Pipeline Derived]` | Derived as `to_date(observation_local_time, "yyyy-MM-dd'T'HH:mm")` — the facility's true local calendar date (partition & join key). | `2026-10-23` |
| `observation_time_utc` | `TIMESTAMP` | `NO` | `[Pipeline Derived]` | True UTC instant computed by converting `observation_local_time` minus `utc_offset_seconds`: `timestamp_seconds(unix_timestamp(observation_local_time, "yyyy-MM-dd'T'HH:mm") - utc_offset_seconds)`. | `2026-10-23 04:15:00.000 UTC` |
| `temperature_2m_c` | `DOUBLE` | `NO` | `[API Response]` | Unnested from Open-Meteo `minutely_15.temperature_2m[k]` ($\text{^\circ C}$). | `26.5` |
| `precipitation_mm` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `minutely_15.precipitation[k]` ($\text{mm}$). | `0.0` |
| `wind_speed_10m_kmh` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `minutely_15.wind_speed_10m[k]` ($\text{km/h}$). | `19.8` |
| `weather_code` | `INT` | `YES` | `[API Response]` | Unnested from Open-Meteo `minutely_15.weather_code[k]` (WMO 4677 code, including `97`). | `1` |
| `weather_description` | `STRING` | `NO` | `[Pipeline Derived]` | Mapped from `weather_code` using `WMO_CODE_MAP`. | `"Mainly clear"` |
| `row_value_hash` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of weather measurement columns (`temperature_2m_c`, `precipitation_mm`, `wind_speed_10m_kmh`, `weather_code`) used in `MERGE` guard to skip no-op updates. | `"7d865e959b2466918c9863afca942d0f..."` |
| `source_ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to `bronze.weather_minutely_15.ingestion_id`. | `"m15-71e9204-882a-4c11-90ef-123456789abc"` |
| `api_ingested_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Propagated from `bronze.weather_minutely_15.ingested_at_utc`. | `2026-10-23 00:05:15.100 UTC` |
| `updated_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Current UTC timestamp set when inserted or updated by `MERGE INTO`. | `2026-10-23 00:05:17.500 UTC` |

---

### 3.3 Table: `silver.fact_weather_hourly`
* **Objective:** Conformed 1-hour grain weather time-series unnested from `bronze.weather_hourly`, storing both facility local date/time and true UTC time (`observation_time_utc`).
* **Storage Format & Update Strategy:** **Delta Lake — SCD Type 1 Upsert (3-Guard `MERGE INTO` + Change Data Feed `delta.enableChangeDataFeed = true`)**, partitioned by `observation_local_date`.
  * *Sliding 14-Day Window Lifecycle (`ADR-003`):* Older history ($< T-7$) is untouched and retained permanently; overlapping timestamps ($[T-7, T+5]$) are updated in-place (`WHEN MATCHED`) when newer forecasts or assimilated actuals differ in `row_value_hash` (with prior versions logged in Delta CDF `_change_data`); new horizon timestamps ($T+6$) are appended (`WHEN NOT MATCHED THEN INSERT`).
* **Grain:** One row per `(location_id, observation_local_time)` / `(location_id, observation_time_utc)`.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Propagated from `bronze.weather_hourly.location_id`. | `"AU-NSW-SYD-01"` |
| `observation_local_time` | `STRING` | `NO` | `[API Response]` | Local ISO8601 timestamp string `hourly.time[k]` in the facility's local timezone. | `"2026-10-23T15:00"` |
| `observation_local_date` | `DATE` | `NO` | `[Pipeline Derived]` | Derived as `to_date(observation_local_time, "yyyy-MM-dd'T'HH:mm")` — matches the local date grain of `silver.fact_weather_daily`. | `2026-10-23` |
| `observation_time_utc` | `TIMESTAMP` | `NO` | `[Pipeline Derived]` | True UTC instant computed as `timestamp_seconds(unix_timestamp(observation_local_time, "yyyy-MM-dd'T'HH:mm") - utc_offset_seconds)`. | `2026-10-23 04:00:00.000 UTC` |
| `temperature_2m_c` | `DOUBLE` | `NO` | `[API Response]` | Unnested from Open-Meteo `hourly.temperature_2m[k]` ($\text{^\circ C}$). | `26.4` |
| `apparent_temperature_c` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.apparent_temperature[k]` ($\text{^\circ C}$). | `27.8` |
| `relative_humidity_pct` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.relative_humidity_2m[k]` ($\%$). | `62.0` |
| `precipitation_mm` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.precipitation[k]` ($\text{mm}$). | `0.0` |
| `precipitation_prob_pct` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.precipitation_probability[k]` ($\%$). | `10.0` |
| `wind_speed_10m_kmh` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.wind_speed_10m[k]` ($\text{km/h}$). | `19.5` |
| `surface_pressure_hpa` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.surface_pressure[k]` ($\text{hPa}$). | `1014.2` |
| `uv_index` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.uv_index[k]`. | `7.2` |
| `weather_code` | `INT` | `YES` | `[API Response]` | Unnested from Open-Meteo `hourly.weather_code[k]` (including `97`). | `1` |
| `weather_description` | `STRING` | `NO` | `[Pipeline Derived]` | Mapped from `weather_code` via `WMO_CODE_MAP`. | `"Mainly clear"` |
| `row_value_hash` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of all hourly weather measurement columns used in the `MERGE` change-detection guard. | `"b5bb9d8014a0f9b1d61e21e796d78dcc..."` |
| `source_ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to `bronze.weather_hourly.ingestion_id`. | `"hr-b71e9204-882a-4c11-90ef-123456789abc"` |
| `api_ingested_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Propagated from `bronze.weather_hourly.ingested_at_utc`. | `2026-10-23 00:05:15.250 UTC` |
| `updated_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Current UTC timestamp set when inserted or updated by `MERGE INTO`. | `2026-10-23 00:05:18.400 UTC` |

---

### 3.4 Table: `silver.fact_weather_daily`
* **Objective:** Conformed daily grain weather time-series unnested from `bronze.weather_daily` (aggregated over each facility's local 24h calendar day `observation_local_date`), enriched with derived diurnal temperature swing.
* **Storage Format & Update Strategy:** **Delta Lake — SCD Type 1 Upsert (3-Guard `MERGE INTO` + Change Data Feed `delta.enableChangeDataFeed = true`)** on `(location_id, observation_local_date)`.
  * *Sliding 14-Day Window Lifecycle (`ADR-003`):* Older history ($< T-7$) is untouched and retained permanently; overlapping dates ($[T-7, T+5]$) are updated in-place (`WHEN MATCHED`) when newer forecasts or assimilated actuals differ in `row_value_hash` (with prior versions logged in Delta CDF `_change_data`); the new forward horizon date ($T+6$) is appended (`WHEN NOT MATCHED THEN INSERT`).
* **Grain:** One row per `(location_id, observation_local_date)`.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Propagated from `bronze.weather_daily.location_id`. | `"AU-VIC-MEL-01"` |
| `observation_local_date` | `DATE` | `NO` | `[API Response]` | Parsed via `to_date(daily.time[k], "yyyy-MM-dd")` from Open-Meteo's `daily.time` array (local calendar date at the facility). | `2026-10-23` |
| `temp_max_c` | `DOUBLE` | `NO` | `[API Response]` | Unnested from Open-Meteo `daily.temperature_2m_max[k]` ($\text{^\circ C}$). | `29.1` |
| `temp_min_c` | `DOUBLE` | `NO` | `[API Response]` | Unnested from Open-Meteo `daily.temperature_2m_min[k]` ($\text{^\circ C}$). | `13.4` |
| `diurnal_temp_range_c` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Calculated in PySpark as `round(temp_max_c - temp_min_c, 2)`. | `15.7` |
| `apparent_temp_max_c` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `daily.apparent_temperature_max[k]` ($\text{^\circ C}$). | `30.5` |
| `apparent_temp_min_c` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `daily.apparent_temperature_min[k]` ($\text{^\circ C}$). | `11.8` |
| `precipitation_sum_mm` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `daily.precipitation_sum[k]` ($\text{mm}$). | `2.4` |
| `wind_speed_max_kmh` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `daily.wind_speed_10m_max[k]` ($\text{km/h}$). | `42.0` |
| `uv_index_max` | `DOUBLE` | `YES` | `[API Response]` | Unnested from Open-Meteo `daily.uv_index_max[k]`. | `8.5` |
| `weather_code` | `INT` | `YES` | `[API Response]` | Unnested from Open-Meteo `daily.weather_code[k]` (including `97`). | `61` |
| `weather_description` | `STRING` | `NO` | `[Pipeline Derived]` | Mapped from `weather_code` via `WMO_CODE_MAP`. | `"Rain: Slight intensity"` |
| `row_value_hash` | `STRING` | `NO` | `[Pipeline Derived]` | SHA-256 hash of daily weather measurement columns used in the `MERGE` change-detection guard. | `"c3ab8ff13720e8ad9047dd39466b3c89..."` |
| `source_ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to `bronze.weather_daily.ingestion_id`. | `"dy-a12b34c5-882a-4c11-90ef-123456789abc"` |
| `api_ingested_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Propagated from `bronze.weather_daily.ingested_at_utc`. | `2026-10-23 00:05:15.400 UTC` |
| `updated_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | Current UTC timestamp set when inserted or updated by `MERGE INTO`. | `2026-10-23 00:05:19.120 UTC` |

---

### 3.5 Table: `silver.quarantine_weather_records`
* **Objective:** Dead-letter / quarantine table populated by the Data Quality engine (`src/quality/validators.py`) whenever an incremental Bronze payload or unnested row violates any of the 13 DQ rules.
* **Storage Format & Update Strategy:** Delta Lake (`APPEND` only).
* **Grain:** One row per quarantined record failure event.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `quarantine_id` | `STRING` | `NO` | `[System Metadata]` | UUID generated when the rejected record is written to quarantine. | `"q11a22b3-44c5-66d7-88e9-00f112233445"` |
| `pipeline_run_id` | `STRING` | `NO` | `[System Metadata]` | Active pipeline execution run ID. | `"run-20261023-070512-8f3a"` |
| `source_bronze_table` | `STRING` | `NO` | `[Pipeline Derived]` | Originating Bronze table (`bronze.weather_minutely_15`, `bronze.weather_hourly`, or `bronze.weather_daily`). | `"bronze.weather_hourly"` |
| `source_ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Foreign key to the originating Bronze table's `ingestion_id`. | `"hr-b71e9204-882a-4c11-90ef-123456789abc"` |
| `target_silver_table` | `STRING` | `NO` | `[Pipeline Derived]` | Target Silver table name (`silver.fact_weather_hourly`, etc.). | `"silver.fact_weather_hourly"` |
| `location_id` | `STRING` | `YES` | `[Seed Config: locations.json]` | Propagated `location_id` of the rejected record. | `"AU-NSW-SYD-01"` |
| `observation_timestamp_raw` | `STRING` | `YES` | `[API Response]` | Raw local timestamp/date string from the API array (`time[k]`) for the rejected row. | `"2026-10-23T15:00"` |
| `failed_record_json` | `STRING` | `NO` | `[Pipeline Derived]` | Full JSON serialization of all candidate columns in the rejected row. | `'{"observation_local_time":"2026-10-23T15:00","temperature_2m_c":145.0}'` |
| `dq_rule_violated` | `STRING` | `NO` | `[Pipeline Derived]` | Comma-separated list of violated rule codes from `data_quality_checks.md`. | `"DQ-PHY-001,DQ-PHY-002"` |
| `dq_failure_details` | `STRING` | `NO` | `[Pipeline Derived]` | Human-readable diagnostic string explaining the exact column values and violated thresholds. | `"temperature_2m_c (145.0) outside [-50.0, 60.0]; relative_humidity_pct (112.0) outside [0.0, 100.0]"` |
| `quarantined_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when the record was appended to quarantine. | `2026-10-23 00:05:18.250 UTC` |

---

## 4. Gold Layer (`gold`)

### 4.1 Table: `gold.mart_daily_weather_summary`
* **Objective:** Business-ready analytical mart joining `silver.fact_weather_daily`, aggregated `silver.fact_weather_hourly` statistics (aligned on facility local date `observation_local_date`), and the active `silver.dim_location_scd2` (`is_current = true`). Dynamically classifies each date relative to the facility's local calendar date (`facility_local_today`) into `PAST_MODELLED`, `TODAY_MIXED`, or `FORWARD_FORECAST`.
* **Storage Format & Update Strategy:** Delta Lake (`MERGE INTO` on `(location_id, observation_local_date)`).
* **Grain:** One row per `(location_id, observation_local_date)`.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Joined from `silver.fact_weather_daily.location_id`. | `"AU-NSW-SYD-01"` |
| `location_sk` | `STRING` | `NO` | `[Pipeline Derived]` | Joined from active `silver.dim_location_scd2.location_sk` (`WHERE is_current = true`). | `"8f9d2a1c4b6e7f0a123456789abcdef0..."` |
| `location_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Joined from `silver.dim_location_scd2.location_name`. | `"Sydney Metro Care Hub"` |
| `state_code` | `STRING` | `NO` | `[Seed Config: locations.json]` | Joined from `silver.dim_location_scd2.state_code`. | `"NSW"` |
| `timezone_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Joined from `silver.dim_location_scd2.timezone_name`. | `"Australia/Sydney"` |
| `observation_local_date` | `DATE` | `NO` | `[API Response]` | Facility local calendar date from `silver.fact_weather_daily.observation_local_date`. | `2026-10-23` |
| `is_forecast` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | Computed dynamically at Gold build time against the facility's local current date (`facility_local_today = to_date(from_utc_timestamp(current_timestamp(), timezone_name))`): `true` if `observation_local_date >= facility_local_today`, else `false`. | `true` |
| `data_horizon_type` | `STRING` | `NO` | `[Pipeline Derived]` | Three-state temporal classification relative to `facility_local_today`: `"PAST_MODELLED"` if `observation_local_date < facility_local_today`, `"TODAY_MIXED"` if `observation_local_date == facility_local_today`, and `"FORWARD_FORECAST"` if `observation_local_date > facility_local_today`. | `"TODAY_MIXED"` |
| `temp_max_c` | `DOUBLE` | `NO` | `[API Response]` | From `silver.fact_weather_daily.temp_max_c`. | `28.5` |
| `temp_min_c` | `DOUBLE` | `NO` | `[API Response]` | From `silver.fact_weather_daily.temp_min_c`. | `16.2` |
| `diurnal_temp_range_c` | `DOUBLE` | `NO` | `[Pipeline Derived]` | From `silver.fact_weather_daily.diurnal_temp_range_c`. | `12.3` |
| `hourly_avg_temp_c` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Computed via `ROUND(AVG(temperature_2m_c), 2)` grouped by `(location_id, observation_local_date)` from `silver.fact_weather_hourly`. | `21.8` |
| `hourly_avg_humidity_pct` | `DOUBLE` | `YES` | `[Pipeline Derived]` | Computed via `ROUND(AVG(relative_humidity_pct), 2)` grouped by `(location_id, observation_local_date)` from `silver.fact_weather_hourly`. | `64.5` |
| `rolling_7d_avg_temp_c` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Computed via PySpark Window function: `ROUND(AVG(hourly_avg_temp_c) OVER (PARTITION BY location_id ORDER BY observation_local_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)`. | `19.4` |
| `temp_anomaly_vs_7d_c` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Computed as `ROUND(hourly_avg_temp_c - rolling_7d_avg_temp_c, 2)`. | `+2.4` |
| `rolling_7d_precip_mm` | `DOUBLE` | `NO` | `[Pipeline Derived]` | Computed via PySpark Window function: `ROUND(SUM(precipitation_sum_mm) OVER (PARTITION BY location_id ORDER BY observation_local_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)`. | `18.6` |
| `extreme_heat_hours_count` | `INT` | `NO` | `[Pipeline Derived]` | Count of hours in `silver.fact_weather_hourly` on `observation_local_date` where `temperature_2m_c >= ALERT_HEATWAVE_TEMP_C`. | `0` |
| `extreme_cold_hours_count` | `INT` | `NO` | `[Pipeline Derived]` | Count of hours in `silver.fact_weather_hourly` on `observation_local_date` where `temperature_2m_c <= ALERT_COLD_SNAP_TEMP_C`. | `0` |
| `dominant_weather_condition` | `STRING` | `NO` | `[Pipeline Derived]` | Propagated from `silver.fact_weather_daily.weather_description`. | `"Partly cloudy"` |
| `mart_updated_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when the Gold mart was materialized. | `2026-10-23 00:05:22.100 UTC` |

---

### 4.2 Table: `gold.mart_care_facility_risk_alerts`
* **Objective:** Domain-specific operational alerting mart tailored for Australian Aged Care operations. Evaluates daily and hourly metrics against configurable `.env` thresholds, dynamically tagging each alert with `is_forecast` and `data_horizon_type` relative to the facility's local timezone.
* **Storage Format & Update Strategy:** Delta Lake (`MERGE INTO` on `(location_id, observation_local_date)`).
* **Grain:** One row per `(location_id, observation_local_date)`.

| Column Name | Data Type | Nullable | Origin / Source | Description & How Obtained / Created | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `alert_id` | `STRING` | `NO` | `[Pipeline Derived]` | Deterministic primary key computed as `sha2(concat_ws('||', location_id, cast(observation_local_date as string)), 256)`. | `"3c9b1e2f8a7d6c5b4a3f2e1d0c9b8a7f..."` |
| `location_id` | `STRING` | `NO` | `[Seed Config: locations.json]` | Facility hub identifier from `silver.dim_location_scd2`. | `"AU-NT-DRW-01"` |
| `location_name` | `STRING` | `NO` | `[Seed Config: locations.json]` | Facility hub name from `silver.dim_location_scd2`. | `"Darwin Top End Care Hub"` |
| `state_code` | `STRING` | `NO` | `[Seed Config: locations.json]` | Australian state/territory code from `silver.dim_location_scd2`. | `"NT"` |
| `climate_risk_tier` | `STRING` | `NO` | `[Seed Config: locations.json]` | Facility baseline vulnerability tier from `silver.dim_location_scd2`. | `"TROPICAL"` |
| `observation_local_date` | `DATE` | `NO` | `[API Response]` | Local calendar date at the facility from `silver.fact_weather_daily.observation_local_date`. | `2026-10-25` |
| `is_forecast` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | Dynamically evaluated at Gold build time against the facility's local date (`observation_local_date >= facility_local_today`). | `true` |
| `data_horizon_type` | `STRING` | `NO` | `[Pipeline Derived]` | `"PAST_MODELLED"`, `"TODAY_MIXED"`, or `"FORWARD_FORECAST"` relative to `facility_local_today`. | `"FORWARD_FORECAST"` |
| `overall_risk_severity` | `STRING` | `NO` | `[Pipeline Derived]` | Rule-based classification: `"CRITICAL"` if $\ge 2$ hazard flags are `true` or `temp_max_c >= ALERT_HEATWAVE_TEMP_C + 5`; `"WARNING"` if $1$ major hazard flag is `true`; `"WATCH"` if within $2^\circ\text{C}$ of threshold; else `"NORMAL"`. | `"WARNING"` |
| `is_heatwave_risk` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | `temp_max_c >= .env.ALERT_HEATWAVE_TEMP_C` ($35^\circ\text{C}$) OR `apparent_temp_max_c >= 37.0`. | `true` |
| `is_cold_snap_risk` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | `temp_min_c <= .env.ALERT_COLD_SNAP_TEMP_C` ($5^\circ\text{C}$). | `false` |
| `is_severe_storm_risk` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | `wind_speed_max_kmh >= .env.ALERT_HIGH_WIND_KMH` ($60\text{ km/h}$) OR `precipitation_sum_mm >= 25.0` OR `weather_code IN (95, 96, 97, 99)`. | `false` |
| `is_high_uv_risk` | `BOOLEAN` | `NO` | `[Pipeline Derived]` | `uv_index_max >= .env.ALERT_HIGH_UV_INDEX` ($8.0$). | `true` |
| `primary_hazard_reason` | `STRING` | `NO` | `[Pipeline Derived]` | Concatenated diagnostic string summarizing all triggered hazard flags and their measured values. | `"Extreme Heat (Max 36.2°C / Feels-like 39.4°C) & Extreme UV (11.2)"` |
| `recommended_care_action` | `STRING` | `NO` | `[Pipeline Derived]` | Mapped from triggered risk flags to standard aged-care operational protocols. | `"Initiate Heatwave Hydration Protocol; check HVAC cooling in resident wings; restrict outdoor activities 10:00-16:00."` |
| `source_ingestion_id` | `STRING` | `NO` | `[System Metadata]` | Lineage foreign key to `bronze.weather_daily.ingestion_id`. | `"dy-a12b34c5-882a-4c11-90ef-123456789abc"` |
| `evaluated_at_utc` | `TIMESTAMP` | `NO` | `[System Metadata]` | UTC timestamp when the Gold risk evaluation was executed. | `2026-10-23 00:05:23.450 UTC` |
