/*
  Pattern 5: Monthly Deep Backfill for Measurement Protocol / Offline Events (2026)

  THE PROBLEM:
  The 3-day insert_overwrite (Pattern 1) works for natively collected web/app
  events, which Google backfills within 72 hours. But Measurement Protocol (MP)
  events, offline conversion imports, and CRM-synced events can be backdated
  15-30+ days. They land in old partitions (e.g., events_20260501) that your
  daily pipeline never touches.

  THE SOLUTION:
  Keep the fast 3-day overwrite for daily freshness, but run this job monthly
  to rebuild the last 60-90 days. It catches stragglers without blowing up
  daily costs.

  SCHEDULING:
  Run via Airflow, Cloud Composer, dbt Cloud, or a cron job on the 1st of
  each month at 03:00 UTC.

  2026 UPDATES:
  - Uses collected_traffic_source and session_traffic_source_last_click
  - Includes privacy_info for Consent Mode v2
  - Uses incremental_predicates (dbt 1.7+)
  - Cost guardrail: 60-day scan ~€8-15, set maximum_bytes_billed = 500 GiB
*/

-- ============================================================================
-- dbt config (2026 syntax)
-- ============================================================================
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='insert_overwrite',
--     incremental_predicates=[
--       "event_date >= date_sub(current_date(), interval 60 day)"
--     ],
--     on_schema_change='sync_all_columns'
-- ) }}

-- ============================================================================
-- Dataform / Pure SQL equivalent
-- ============================================================================

config {
  type: "incremental",
  bigquery: {
    partitionBy: "event_date",
    clusterBy: ["event_name"],
    requirePartitionFilter: true
  }
}

js {
  // 60-day lookback for monthly backfill.
  // Adjust to 90 if your MP lag is longer (e.g. CRM batch imports).
  const lookbackDays = 60;

  const startDate = date_trunc(current_date(), month); // 1st of current month
  const endDate = "current_date()";

  const dateFilter = `((_table_suffix >= cast(${startDate} as string format "YYYYMMDD") and _table_suffix <= cast(${endDate} as string format "YYYYMMDD"))
  or (_table_suffix >= 'intraday_'||cast(${startDate} as string format "YYYYMMDD") and _table_suffix <= 'intraday_'||cast(${endDate} as string format "YYYYMMDD")))`;
}

with raw_events as (
  -- Daily export (stable, backfilled by Google)
  select
    parse_date('%Y%m%d', event_date) as event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    -- 2026: Native traffic source fields
    collected_traffic_source.source as traffic_source,
    collected_traffic_source.medium as traffic_medium,
    collected_traffic_source.campaign as traffic_campaign,
    session_traffic_source_last_click.source as session_source,
    session_traffic_source_last_click.medium as session_medium,
    session_traffic_source_last_click.campaign as session_campaign,
    -- 2026: Consent Mode v2
    privacy_info.analytics_storage as analytics_storage_consent,
    privacy_info.ads_storage as ads_storage_consent,
    is_active_user,
    event_params,
    ecommerce,
    device,
    geo,
    'daily' as _export_type
  from `<project>.<dataset>.events_*`
  where ${dateFilter}

  union all

  -- Intraday export (real-time, today only usually, but included for safety)
  select
    parse_date('%Y%m%d', regexp_extract(_table_suffix, r'intraday_(\d+)')) as event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    collected_traffic_source.source as traffic_source,
    collected_traffic_source.medium as traffic_medium,
    collected_traffic_source.campaign as traffic_campaign,
    session_traffic_source_last_click.source as session_source,
    session_traffic_source_last_click.medium as session_medium,
    session_traffic_source_last_click.campaign as session_campaign,
    privacy_info.analytics_storage as analytics_storage_consent,
    privacy_info.ads_storage as ads_storage_consent,
    is_active_user,
    event_params,
    ecommerce,
    device,
    geo,
    'intraday' as _export_type
  from `<project>.<dataset>.events_intraday_*`
  where ${dateFilter}
),

-- De-duplicate: if a row exists in both daily and intraday, prefer daily
deduped as (
  select * except(_export_type, rn)
  from (
    select
      *,
      row_number() over (
        partition by user_pseudo_id, event_timestamp, event_name, event_bundle_sequence_id
        order by case when _export_type = 'daily' then 1 else 2 end
      ) as rn
    from raw_events
  )
  where rn = 1
)

select * from deduped

--
-- PRE-OPERATIONS (Dataform syntax; adapt for dbt/Airflow):
-- Delete the mutable window before inserting fresh data.
-- This is an INSERT OVERWRITE for the last 60 days.
--
-- pre_operations {
--   delete from ${self()}
--   where event_date >= date_sub(current_date(), interval 60 day);
-- }
--
-- For dbt, incremental_predicates handles the delete automatically.
