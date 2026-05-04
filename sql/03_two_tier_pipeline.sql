/*
  Pattern 3: Two-Tier Architecture (Production-Grade)

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

  Benefits:
    - Schema drift in GA4 only breaks one model
    - Downstream models are cheap (1-day incrementals)
    - New analysts don't need to understand intraday vs daily
    - Easy to swap GA4 for another event source later
*/

-- ============================================================================
-- TIER 1: SOURCE MART
-- ============================================================================

-- dbt config:
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='insert_overwrite',
--     partitions=[
--         "date_sub(current_date(), interval 3 day)",
--         "date_sub(current_date(), interval 2 day)",
--         "date_sub(current_date(), interval 1 day)",
--         "current_date()"
--     ],
--     on_schema_change='sync_all_columns'
-- ) }}

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
    event_previous_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec,
    ecommerce,
    device,
    geo,
    traffic_source,
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
    event_previous_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') AS engagement_time_msec,
    ecommerce,
    device,
    geo,
    traffic_source,
    'intraday' AS _export_type
  FROM `project.analytics_123456789.events_intraday_*`
  WHERE _table_suffix BETWEEN
    format_date('intraday_%Y%m%d', date_sub(current_date(), interval 3 day))
    AND format_date('intraday_%Y%m%d', current_date())
)

SELECT
  * EXCEPT(_export_type, rn)
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, event_timestamp, event_name
      ORDER BY CASE WHEN _export_type = 'daily' THEN 1 ELSE 2 END
    ) AS rn
  FROM raw
)
WHERE rn = 1;


-- ============================================================================
-- TIER 2: EXAMPLE BUSINESS MODEL (Session Facts)
-- ============================================================================

-- dbt config:
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='insert_overwrite',
--     partitions=["current_date()"]
-- ) }}

-- Pure SQL: only process today's stable data from the source mart
DELETE FROM `project.dataset.fct_ga4_sessions`
WHERE event_date = current_date();

INSERT INTO `project.dataset.fct_ga4_sessions`
WITH session_events AS (
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    event_name,
    event_timestamp,
    page_location,
    page_referrer,
    -- Extract utm params from page_location for attribution
    REGEXP_EXTRACT(page_location, r'[?&]utm_source=([^&]+)') AS utm_source,
    REGEXP_EXTRACT(page_location, r'[?&]utm_medium=([^&]+)') AS utm_medium,
    REGEXP_EXTRACT(page_location, r'[?&]utm_campaign=([^&]+)') AS utm_campaign,
    engagement_time_msec
  FROM `project.dataset.stg_ga4__events`
  WHERE event_date = current_date()
)

SELECT
  event_date,
  user_pseudo_id,
  ga_session_id,
  -- First hit of session sets attribution (GA4 rule)
  FIRST_VALUE(utm_source) OVER (
    PARTITION BY user_pseudo_id, ga_session_id
    ORDER BY event_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS session_source,
  FIRST_VALUE(utm_medium) OVER (
    PARTITION BY user_pseudo_id, ga_session_id
    ORDER BY event_timestamp
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS session_medium,
  COUNTIF(event_name = 'page_view') AS page_views,
  COUNTIF(event_name = 'purchase') AS purchases,
  SUM(engagement_time_msec) / 1000.0 AS engagement_time_sec,
  MIN(event_timestamp) AS session_start_timestamp,
  MAX(event_timestamp) AS session_end_timestamp
FROM session_events
GROUP BY 1, 2, 3;

/*
  COST COMPARISON for this two-tier setup (50M events/month):

  Tier 1 (source mart):  3-day overwrite = ~0.60/day
  Tier 2 (sessions):     1-day overwrite = ~0.05/day
  Tier 2 (conversions):  1-day overwrite = ~0.03/day
  Tier 2 (LTV):          1-day overwrite = ~0.08/day
  --------------------------------------------------------
  Total daily:           ~0.76/day = ~23/month

  Without two-tier (each model unions raw GA4 tables):
  4 models × 3-day raw scan = ~2.40/day = ~72/month

  Savings: ~65% at scale. The gap widens as you add more models.
*/
