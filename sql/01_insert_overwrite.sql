/*
  Dataform configuration (save as .sqlx in your Dataform project):

  config {
    type: "incremental",
    schema: "analytics",
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["event_name"],
      requirePartitionFilter: true
    }
  }

  js {
    const lookbackDays = 3;
  }

  pre_operations {
    delete from ${self()} where event_date >= date_sub(current_date(), interval 3 day);
  }
*/

-- ============================================================================
-- Pattern 1: Insert Overwrite (Recommended for Dataform / BigQuery)
-- ============================================================================
-- Replace the last 3 days of data on every run. Leave everything older untouched.
-- This handles:
--   - Intraday updates (today's data changes hourly)
--   - Daily export backfills (Google re-processes yesterday with corrections)
--   - Late-arriving native events (up to 72h delay)
--   - Measurement Protocol events (see Pattern 5 for backdated MP)
--
-- WHY NOT MERGE?
-- BigQuery MERGE scans the entire target partition to find matching rows.
-- On 100M+ row tables, that's €3-5/day vs €0.40-0.80 for insert overwrite.
-- At scale, this matters.
--
-- 2026 UPDATES vs 2023-era advice:
-- - Uses native collected_traffic_source (not event_params parsing)
-- - Uses session_traffic_source_last_click for reliable attribution
-- - Includes privacy_info for Consent Mode v2 (mandatory EEA since 2024)
-- - Includes is_active_user for engaged vs bounce distinction
-- - Cost guardrail: always set maximum_bytes_billed in your Dataform config
-- ============================================================================

WITH raw_events AS (
  -- Daily export: complete, stable, 24h latency
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    -- 2026: Native traffic_source fields (stop parsing event_params)
    collected_traffic_source.source AS traffic_source,
    collected_traffic_source.medium AS traffic_medium,
    collected_traffic_source.campaign AS traffic_campaign,
    -- 2026: session_traffic_source_last_click is the most reliable
    -- session-level attribution field in BigQuery
    session_traffic_source_last_click.source AS session_source,
    session_traffic_source_last_click.medium AS session_medium,
    session_traffic_source_last_click.campaign AS session_campaign,
    -- 2026: privacy_info is critical for Consent Mode v2 (EEA traffic)
    privacy_info.analytics_storage AS analytics_storage_consent,
    privacy_info.ads_storage AS ads_storage_consent,
    -- 2026: is_active_user distinguishes engaged from bounce sessions
    is_active_user,
    -- Safe access: intraday lacks some attribution fields
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    ecommerce,
    'daily' AS _export_type
  FROM `project.analytics_123456789.events_*`
  WHERE _table_suffix BETWEEN
    FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())

  UNION ALL

  -- Intraday export: real-time, 1h latency, incomplete
  SELECT
    PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(_table_suffix, r'intraday_(\d+)')) AS event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    collected_traffic_source.source AS traffic_source,
    collected_traffic_source.medium AS traffic_medium,
    collected_traffic_source.campaign AS traffic_campaign,
    session_traffic_source_last_click.source AS session_source,
    session_traffic_source_last_click.medium AS session_medium,
    session_traffic_source_last_click.campaign AS session_campaign,
    privacy_info.analytics_storage AS analytics_storage_consent,
    privacy_info.ads_storage AS ads_storage_consent,
    is_active_user,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    ecommerce,
    'intraday' AS _export_type
  FROM `project.analytics_123456789.events_intraday_*`
  WHERE _table_suffix BETWEEN
    FORMAT_DATE('intraday_%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
    AND FORMAT_DATE('intraday_%Y%m%d', CURRENT_DATE())
),

-- De-duplicate: if a row exists in both daily and intraday, keep daily.
-- The daily export is more complete (has attribution fields intraday lacks).
-- NOTE: user_pseudo_id + event_timestamp + event_name is NOT guaranteed unique.
-- For true dedup, add event_bundle_sequence_id or accept near-duplicates.
deduped AS (
  SELECT * EXCEPT(_export_type, rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY user_pseudo_id, event_timestamp, event_name, event_bundle_sequence_id
        ORDER BY CASE WHEN _export_type = 'daily' THEN 1 ELSE 2 END
      ) AS rn
    FROM raw_events
  )
  WHERE rn = 1
)

SELECT * FROM deduped;
