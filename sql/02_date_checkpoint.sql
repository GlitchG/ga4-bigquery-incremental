/*
  Pattern 2: Date Checkpoint (Append-Only, 2026)

  Track the maximum event_date already loaded, then append only new days.
  This is the cheapest option — but it ONLY works when data never changes
  after initial export. For GA4, that is almost never true (72h backfill,
  intraday updates, Measurement Protocol). I rarely use this in production.

  2026 UPDATE:
  - Uses incremental_predicates (dbt 1.7+)
  - Includes privacy_info for Consent Mode v2
*/

-- ============================================================================
-- dbt config (2026 syntax)
-- ============================================================================
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='append',
--     on_schema_change='sync_all_columns'
-- ) }}
--
-- {% if is_incremental() %}
--   WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
-- {% endif %}

-- ============================================================================
-- Dataform equivalent
-- ============================================================================
config {
  type: "incremental",
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

SELECT
  parse_date('%Y%m%d', event_date) AS event_date,
  user_pseudo_id,
  user_id,
  event_name,
  event_timestamp,
  -- 2026: Native traffic source fields
  collected_traffic_source.source AS traffic_source,
  collected_traffic_source.medium AS traffic_medium,
  collected_traffic_source.campaign AS traffic_campaign,
  session_traffic_source_last_click.source AS session_source,
  session_traffic_source_last_click.medium AS session_medium,
  session_traffic_source_last_click.campaign AS session_campaign,
  -- 2026: Consent Mode v2
  privacy_info.analytics_storage AS analytics_storage_consent,
  is_active_user,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
FROM `<project>.<dataset>.events_*`
WHERE ${dateFilter}

pre_operations {
  -- Date checkpoint: only append days not yet in the table
  declare date_checkpoint default (
    ${when(incremental(),
      `select max(event_date)+1 from \${self()}`,
      `select date_trunc(current_date(), year)`
    )}
  )
}
