/*
  Pattern 3: Two-Tier Architecture (Production-Grade, 2026)

  This is what I deploy for clients. The idea is simple:

  Tier 1 (Source Mart): Handle GA4's quirks once.
    - Union daily + intraday exports
    - De-duplicate intraday → daily overlap
    - 3-day insert overwrite
    - This is the ONLY model that touches raw GA4 tables

  Tier 2 (Business Models): Clean incrementals on 1 day of stable data.
    - Session models, conversion funnels, LTV, cohort analysis
    - Each model increments on 1 day (cheap)
    - 20+ models can run without re-implementing GA4 logic

  2026 UPDATES:
  - Uses collected_traffic_source and session_traffic_source_last_click
    (native BQ fields, not event_params parsing)
  - Includes privacy_info for Consent Mode v2 (mandatory EEA since 2024)
  - Handles the "registration session" user_id attribution bug
  - dbt syntax: incremental_predicates (partitions= deprecated in 1.7+)
  - Cost guardrails: maximum_bytes_billed on all jobs
*/

-- ============================================================================
-- TIER 1: SOURCE MART
-- ============================================================================

-- dbt config (2026 syntax):
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='insert_overwrite',
--     incremental_predicates=[
--       "event_date >= date_sub(current_date(), interval 3 day)"
--     ],
--     on_schema_change='sync_all_columns'
-- ) }}

-- COST GUARDRAIL (set in job/dbt config):
-- maximum_bytes_billed = 107374182400  -- 100 GiB

-- Pure SQL: replace 3-day window
DELETE FROM `project.dataset.stg_ga4__events`
WHERE event_date >= date_sub(current_date(), interval 3 day);

INSERT INTO `project.dataset.stg_ga4__events`
WITH raw AS (
  SELECT
    parse_date('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    event_previous_timestamp,
    -- 2026: Native traffic_source fields (stop parsing event_params)
    collected_traffic_source.source AS traffic_source,
    collected_traffic_source.medium AS traffic_medium,
    collected_traffic_source.campaign AS traffic_campaign,
    session_traffic_source_last_click.source AS session_source,
    session_traffic_source_last_click.medium AS session_medium,
    session_traffic_source_last_click.campaign AS session_campaign,
    -- 2026: Consent Mode v2 fields
    privacy_info.analytics_storage AS analytics_storage_consent,
    privacy_info.ads_storage AS ads_storage_consent,
    is_active_user,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec,
    ecommerce,
    device,
    geo,
    'daily' AS _export_type
  FROM `project.analytics_123456789.events_*`
  WHERE _table_suffix BETWEEN
    format_date('%Y%m%d', date_sub(current_date(), interval 3 day))
    AND format_date('%Y%m%d', current_date())

  UNION ALL

  SELECT
    parse_date('%Y%m%d', regexp_extract(_table_suffix, r'intraday_(\d+)')) AS event_date,
    user_pseudo_id,
    user_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    event_previous_timestamp,
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
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec,
    ecommerce,
    device,
    geo,
    'intraday' AS _export_type
  FROM `project.analytics_123456789.events_intraday_*`
  WHERE _table_suffix BETWEEN
    format_date('intraday_%Y%m%d', date_sub(current_date(), interval 3 day))
    AND format_date('intraday_%Y%m%d', current_date())
)

SELECT * EXCEPT(_export_type, rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, event_timestamp, event_name, event_bundle_sequence_id
      ORDER BY CASE WHEN _export_type = 'daily' THEN 1 ELSE 2 END
    ) AS rn
  FROM raw
)
WHERE rn = 1;


-- ============================================================================
-- TIER 2: EXAMPLE BUSINESS MODEL (Session Facts)
-- ============================================================================

-- dbt config (2026 syntax):
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='insert_overwrite',
--     incremental_predicates=[
--       "event_date >= date_sub(current_date(), interval 1 day)"
--     ]
-- ) }}

-- Pure SQL: only process today's stable data from the source mart
DELETE FROM `project.dataset.fct_ga4_sessions`
WHERE event_date >= date_sub(current_date(), interval 1 day);

INSERT INTO `project.dataset.fct_ga4_sessions`
WITH session_events AS (
  SELECT
    event_date,
    user_pseudo_id,
    user_id,
    ga_session_id,
    event_name,
    event_timestamp,
    page_location,
    page_referrer,
    -- 2026: Use native session_traffic_source_last_click, NOT page_location UTM parsing
    session_source,
    session_medium,
    session_campaign,
    -- 2026: Consent-aware metrics
    analytics_storage_consent,
    is_active_user,
    engagement_time_msec
  FROM `project.dataset.stg_ga4__events`
  WHERE event_date >= date_sub(current_date(), interval 1 day)
),

-- 2026: Handle the "registration session" attribution bug
-- When user_id appears mid-session, GA4 UI resets session source to (direct)/(none).
-- In BigQuery, session_traffic_source_last_click is MORE reliable than the UI,
-- but we still flag these sessions for investigation.
session_flags AS (
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    session_source,
    session_medium,
    session_campaign,
    -- Flag: session has both anonymous and identified hits
    COUNTIF(user_id IS NULL) > 0 AND COUNTIF(user_id IS NOT NULL) > 0 AS has_registration_mid_session,
    -- Flag: session source changed during the session (rare, suspicious)
    COUNT(DISTINCT session_source) > 1 AS has_source_change,
    -- Consent-aware: only count events where analytics_storage was granted
    COUNTIF(event_name = 'page_view' AND analytics_storage_consent = 'Granted') AS page_views_consented,
    COUNTIF(event_name = 'purchase' AND analytics_storage_consent = 'Granted') AS purchases_consented,
    SUM(IF(analytics_storage_consent = 'Granted', engagement_time_msec, 0)) / 1000.0 AS engagement_time_sec_consented,
    -- Raw counts (for comparison with GA4 UI, which includes non-consented)
    COUNTIF(event_name = 'page_view') AS page_views_total,
    COUNTIF(event_name = 'purchase') AS purchases_total,
    SUM(engagement_time_msec) / 1000.0 AS engagement_time_sec_total,
    MIN(event_timestamp) AS session_start_timestamp,
    MAX(event_timestamp) AS session_end_timestamp,
    -- Is this an "active" session per GA4's definition?
    LOGICAL_OR(is_active_user) AS is_active_session
  FROM session_events
  GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
  event_date,
  user_pseudo_id,
  ga_session_id,
  session_source,
  session_medium,
  session_campaign,
  has_registration_mid_session,
  has_source_change,
  -- Use consented metrics as primary (EEA-compliant)
  page_views_consented AS page_views,
  purchases_consented AS purchases,
  engagement_time_sec_consented AS engagement_time_sec,
  -- Keep raw totals for reconciliation with GA4 UI
  page_views_total,
  purchases_total,
  engagement_time_sec_total,
  session_start_timestamp,
  session_end_timestamp,
  is_active_session
FROM session_flags;

/*
  COST COMPARISON for this two-tier setup (50M events/month):

  Tier 1 (source mart):  3-day overwrite = ~€0.60/day
  Tier 2 (sessions):     1-day overwrite = ~€0.05/day
  Tier 2 (conversions):  1-day overwrite = ~€0.03/day
  Tier 2 (LTV):          1-day overwrite = ~€0.08/day
  --------------------------------------------------------
  Total daily:           ~€0.76/day = ~€23/month

  Without two-tier (each model unions raw GA4 tables):
  4 models × 3-day raw scan = ~€2.40/day = ~€72/month

  Savings: ~65% at scale. The gap widens as you add more models.
*/
