/*
  Dataform configuration (save as .sqlx in your Dataform project):

  config {
    type: "incremental",
    schema: "analytics",
    name: "stg_ga4__events_mp_backfill",
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["event_name"],
      requirePartitionFilter: true
    }
  }

  js {
    const lookbackDays = 60;
    const startDate = `date_trunc(current_date(), month)`;
    const endDate = "current_date()";
    const dateFilter = `((_table_suffix >= cast(${startDate} as string format "YYYYMMDD") and _table_suffix <= cast(${endDate} as string format "YYYYMMDD")) or (_table_suffix >= 'intraday_'||cast(${startDate} as string format "YYYYMMDD") and _table_suffix <= 'intraday_'||cast(${endDate} as string format "YYYYMMDD")))`;
  }

  pre_operations {
    delete from ${self()} where event_date >= date_sub(current_date(), interval 60 day);
  }
*/

-- ============================================================================
-- Pattern 5: Monthly Deep Backfill for Measurement Protocol / Offline Events
-- ============================================================================
-- THE PROBLEM:
-- The 3-day insert_overwrite (Pattern 1) works for natively collected web/app
-- events, which Google backfills within 72 hours. But Measurement Protocol (MP)
-- events, offline conversion imports, and CRM-synced events can be backdated
-- 15-30+ days. They land in old partitions (e.g., events_20260501) that your
-- daily pipeline never touches.
--
-- THE SOLUTION:
-- Keep the fast 3-day overwrite for daily freshness, but run this job monthly
-- to rebuild the last 60-90 days. It catches stragglers without blowing up
-- daily costs.
--
-- SCHEDULING:
-- Run via Dataform Workflows on the 1st of each month at 03:00 UTC.
--
-- 2026 UPDATES:
-- - Uses collected_traffic_source and session_traffic_source_last_click
-- - Includes privacy_info for Consent Mode v2
-- - Cost guardrail: 60-day scan ~€8-15, set maximum_bytes_billed = 500 GiB
-- ============================================================================

WITH raw_events AS (
  -- Daily export (stable, backfilled by Google)
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    -- 2026: Native traffic source fields
    collected_traffic_source.source AS traffic_source,
    collected_traffic_source.medium AS traffic_medium,
    collected_traffic_source.campaign AS traffic_campaign,
    session_traffic_source_last_click.source AS session_source,
    session_traffic_source_last_click.medium AS session_medium,
    session_traffic_source_last_click.campaign AS session_campaign,
    -- 2026: Consent Mode v2
    privacy_info.analytics_storage AS analytics_storage_consent,
    privacy_info.ads_storage AS ads_storage_consent,
    is_active_user,
    event_params,
    ecommerce,
    device,
    geo,
    'daily' AS _export_type
  FROM `project.analytics_123456789.events_*`
  WHERE _table_suffix BETWEEN
    FORMAT_DATE('%Y%m%d', DATE_TRUNC(CURRENT_DATE(), MONTH))
    AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())

  UNION ALL

  -- Intraday export (real-time, today only usually, but included for safety)
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
    event_params,
    ecommerce,
    device,
    geo,
    'intraday' AS _export_type
  FROM `project.analytics_123456789.events_intraday_*`
  WHERE _table_suffix BETWEEN
    FORMAT_DATE('intraday_%Y%m%d', DATE_TRUNC(CURRENT_DATE(), MONTH))
    AND FORMAT_DATE('intraday_%Y%m%d', CURRENT_DATE())
),

-- De-duplicate: if a row exists in both daily and intraday, prefer daily
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
