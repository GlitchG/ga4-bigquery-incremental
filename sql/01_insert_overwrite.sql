/*
  Pattern 1: Insert Overwrite (Recommended)
  
  Replace the last 3 days of data on every run. This handles:
  - Intraday updates (today's data changes hourly)
  - Daily export backfills (Google re-processes yesterday with corrections)
  - Late-arriving events (up to 72h delay)
  
  Why not MERGE? BigQuery MERGE scans the entire target partition to find
  matching rows. On 100M+ row tables, that's €3-5/day vs €0.40-0.80 for
  insert overwrite. At scale, this matters.
*/

-- In dbt, this is config:
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='insert_overwrite',
--     partitions=[
--         "date_sub(current_date(), interval 3 day)",
--         "date_sub(current_date(), interval 2 day)",
--         "date_sub(current_date(), interval 1 day)",
--         "current_date()"
--     ]
-- ) }}

-- Pure SQL equivalent (for Dataform, Airflow, or manual runs):
-- Step 1: Delete the mutable window
DELETE FROM `project.dataset.ga4_events_enriched`
WHERE event_date >= date_sub(current_date(), interval 3 day);

-- Step 2: Insert fresh data for the last 3 days
INSERT INTO `project.dataset.ga4_events_enriched`
WITH raw_events AS (
  -- Daily export: complete, stable, 24h latency
  SELECT
    parse_date('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    -- Safe access: intraday lacks some attribution fields
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    ecommerce
  FROM `project.analytics_123456789.events_*`
  WHERE _table_suffix BETWEEN
    format_date('%Y%m%d', date_sub(current_date(), interval 3 day))
    AND format_date('%Y%m%d', current_date())

  UNION ALL

  -- Intraday export: real-time, 1h latency, incomplete
  SELECT
    parse_date('%Y%m%d', regexp_extract(_table_suffix, r'intraday_(\d+)')) AS event_date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    ecommerce
  FROM `project.analytics_123456789.events_intraday_*`
  WHERE _table_suffix BETWEEN
    format_date('intraday_%Y%m%d', date_sub(current_date(), interval 3 day))
    AND format_date('intraday_%Y%m%d', current_date())
),

-- De-duplicate: if a row exists in both daily and intraday, keep daily
-- The daily export is more complete (has attribution fields intraday lacks)
deduped AS (
  SELECT
    event_date,
    user_pseudo_id,
    event_name,
    event_timestamp,
    page_location,
    ga_session_id,
    ecommerce,
    -- Track which export type won (useful for debugging)
    _export_type
  FROM (
    SELECT
      *,
      'daily' AS _export_type,
      ROW_NUMBER() OVER (
        PARTITION BY user_pseudo_id, event_timestamp, event_name
        ORDER BY CASE WHEN _export_type = 'daily' THEN 1 ELSE 2 END
      ) AS rn
    FROM raw_events
  )
  WHERE rn = 1
)

SELECT * FROM deduped;
