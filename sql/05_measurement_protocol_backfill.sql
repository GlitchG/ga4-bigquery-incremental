/*
  Pattern 5: Monthly Deep Backfill for Measurement Protocol / Offline Events

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
*/

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
  const endDate = 'current_date()';

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
    event_params,
    traffic_source,
    -- ... add your full column list here
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
    event_params,
    traffic_source,
    -- ... same columns
    'intraday' as _export_type
  from `<project>.<dataset>.events_intraday_*`
  where ${dateFilter}
),

-- De-duplicate: if a row exists in both daily and intraday, prefer daily
deduped as (
  select * except(_export_type)
  from raw_events
  qualify row_number() over (
    partition by user_pseudo_id, event_timestamp, event_name
    order by case when _export_type = 'daily' then 1 else 2 end
  ) = 1
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
-- For dbt, use:
--   incremental_strategy='insert_overwrite',
--   partitions=[
--     "date_sub(current_date(), interval 60 day)",
--     "date_sub(current_date(), interval 30 day)",
--     ...
--   ]
--