/*
  Dataform configuration (save as .sqlx in your Dataform project):

  config {
    type: "incremental",
    schema: "analytics",
    bigquery: {
      partitionBy: "event_date",
      clusterBy: ["event_name"]
    }
  }

  js {
    const test = false;
    const startDate = test ? "current_date()-5" : "date_checkpoint";
    const endDate = "current_date()";
    const dateFilter = `(_table_suffix >= cast(${startDate} as string format "YYYYMMDD") and _table_suffix <= cast(${endDate} as string format "YYYYMMDD"))`;
  }

  pre_operations {
    declare date_checkpoint default (
      ${when(incremental(),
        `select max(event_date)+1 from \${self()}`,
        `select date_trunc(current_date(), year)`
      )}
    )
  }
*/

-- ============================================================================
-- Pattern 2: Date Checkpoint (Append-Only)
-- ============================================================================
-- Track the maximum event_date already loaded, then append only new days.
-- This is the cheapest option — but it ONLY works when data never changes
-- after initial export. For GA4, that is almost never true (72h backfill,
-- intraday updates, Measurement Protocol). I rarely use this in production.
--
-- 2026 UPDATE:
-- - Uses native collected_traffic_source and session_traffic_source_last_click
-- - Includes privacy_info for Consent Mode v2
-- ============================================================================

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
  -- 2026: Consent Mode v2 fields
  privacy_info.analytics_storage AS analytics_storage_consent,
  is_active_user,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
FROM `project.analytics_123456789.events_*`
WHERE _table_suffix BETWEEN
  FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY))
  AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
