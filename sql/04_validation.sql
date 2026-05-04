/*
  Validation Queries for GA4 Incremental Pipelines

  I run these after every refresh. They catch:
  - Missing data (pipeline didn't run, permissions issue)
  - Schema drift (Google added a new field, existing query broke)
  - Tracking outages (event volume dropped 90% = broken GTM)
  - Duplicate rows (de-duplication failed)
  - Attribution gaps (all sessions direct = UTM params lost)
*/

-- ============================================================================
-- CHECK 1: Row Count Delta vs 7-Day Average
-- ============================================================================

WITH daily_counts AS (
  SELECT
    event_date,
    COUNT(*) AS row_count
  FROM `project.dataset.stg_ga4__events`
  WHERE event_date >= date_sub(current_date(), interval 8 day)
  GROUP BY 1
),
avg_7d AS (
  SELECT AVG(row_count) AS avg_rows
  FROM daily_counts
  WHERE event_date < current_date()
)

SELECT
  d.event_date,
  d.row_count,
  a.avg_rows,
  ROUND((d.row_count - a.avg_rows) / a.avg_rows * 100, 1) AS pct_change
FROM daily_counts d
CROSS JOIN avg_7d a
WHERE d.event_date = current_date()
  -- Alert if today's row count is < 50% or > 200% of the 7-day average
  AND (d.row_count < a.avg_rows * 0.5 OR d.row_count > a.avg_rows * 2.0);

-- ============================================================================
-- CHECK 2: Null Rate on Critical Fields
-- ============================================================================

SELECT
  event_date,
  COUNTIF(user_pseudo_id IS NULL) / COUNT(*) AS pct_null_user_pseudo_id,
  COUNTIF(event_timestamp IS NULL) / COUNT(*) AS pct_null_timestamp,
  COUNTIF(ga_session_id IS NULL) / COUNT(*) AS pct_null_session_id,
  COUNTIF(page_location IS NULL) / COUNT(*) AS pct_null_page_location
FROM `project.dataset.stg_ga4__events`
WHERE event_date >= date_sub(current_date(), interval 3 day)
GROUP BY 1
HAVING pct_null_user_pseudo_id > 0.01
    OR pct_null_timestamp > 0
    OR pct_null_session_id > 0.05;

-- ============================================================================
-- CHECK 3: Event Distribution (Catch Tracking Outages)
-- ============================================================================

WITH daily_events AS (
  SELECT
    event_date,
    event_name,
    COUNT(*) AS event_count
  FROM `project.dataset.stg_ga4__events`
  WHERE event_date >= date_sub(current_date(), interval 7 day)
  GROUP BY 1, 2
),
baseline AS (
  SELECT
    event_name,
    AVG(event_count) AS avg_count
  FROM daily_events
  WHERE event_date < current_date()
  GROUP BY 1
)

SELECT
  d.event_date,
  d.event_name,
  d.event_count,
  b.avg_count,
  ROUND((d.event_count - b.avg_count) / b.avg_count * 100, 1) AS pct_change
FROM daily_events d
JOIN baseline b ON d.event_name = b.event_name
WHERE d.event_date = current_date()
  -- Alert if any event dropped > 70% vs baseline
  AND d.event_count < b.avg_count * 0.3
ORDER BY pct_change;

-- ============================================================================
-- CHECK 4: Duplicate Rows
-- ============================================================================

SELECT
  event_date,
  user_pseudo_id,
  event_timestamp,
  event_name,
  COUNT(*) AS duplicate_count
FROM `project.dataset.stg_ga4__events`
WHERE event_date >= date_sub(current_date(), interval 3 day)
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;

-- ============================================================================
-- CHECK 5: Session Attribution (Not All Direct)
-- ============================================================================

SELECT
  event_date,
  COUNTIF(traffic_source.source = '(direct)' AND traffic_source.medium = '(none)') AS direct_sessions,
  COUNT(*) AS total_sessions,
  ROUND(
    COUNTIF(traffic_source.source = '(direct)' AND traffic_source.medium = '(none)') / COUNT(*) * 100,
    1
  ) AS pct_direct
FROM (
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    traffic_source,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp) AS rn
  FROM `project.dataset.stg_ga4__events`
  WHERE event_date >= date_sub(current_date(), interval 3 day)
)
WHERE rn = 1  -- First hit of each session
GROUP BY 1
HAVING pct_direct > 80;

/*
  If pct_direct > 80%, investigate immediately. Common causes:
  - UTM parameters stripped during login/registration flow
  - Cross-domain tracking not configured
  - campaign_source / campaign_medium fields override URL UTMs mid-session
    (see: https://gtm-gear.com/ga4-sessions-source-medium/)
*/
