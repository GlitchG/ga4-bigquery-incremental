/*
  Validation Queries for GA4 Incremental Pipelines (2026)

  I run these after every refresh. They catch:
  - Missing data (pipeline didn't run, permissions issue)
  - Schema drift (Google added a new field, existing query broke)
  - Tracking outages (event volume dropped 90% = broken GTM)
  - Duplicate rows (de-duplication failed)
  - Attribution gaps (all sessions direct = UTM params lost)
  - Consent Mode issues (analytics_storage = 'Denied' spikes)
  - Cost overruns (query scanned 10x expected bytes)
*/

-- ============================================================================
-- CHECK 0: Cost Guardrail (run this FIRST)
-- ============================================================================

-- In BigQuery, query the INFORMATION_SCHEMA to verify your job stayed under budget:
SELECT
  job_id,
  creation_time,
  query,
  total_bytes_billed / 1024 / 1024 / 1024 AS gib_billed,
  total_bytes_billed / 1e12 * 6.25 AS estimated_cost_usd  -- on-demand pricing
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE job_type = 'QUERY'
  AND creation_time >= timestamp_sub(current_timestamp(), interval 1 hour)
  AND query LIKE '%stg_ga4__events%'
ORDER BY creation_time DESC
LIMIT 10;

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
  -- 2026: Check native traffic_source fields (should not be null on first hit)
  COUNTIF(session_source IS NULL) / COUNT(*) AS pct_null_session_source
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

-- NOTE: True duplicates are rare in GA4. Near-duplicates (same timestamp,
-- different event_bundle_sequence_id) are expected from retries.
-- Only alert if duplicate_count > 2 (indicates a real pipeline bug).
SELECT
  event_date,
  user_pseudo_id,
  event_timestamp,
  event_name,
  event_bundle_sequence_id,
  COUNT(*) AS duplicate_count
FROM `project.dataset.stg_ga4__events`
WHERE event_date >= date_sub(current_date(), interval 3 day)
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) > 2;

-- ============================================================================
-- CHECK 5: Session Attribution (Not All Direct)
-- ============================================================================

-- 2026: Use session_source (from session_traffic_source_last_click) instead
-- of parsing traffic_source from the first hit.
SELECT
  event_date,
  COUNTIF(session_source = '(direct)' AND session_medium = '(none)') AS direct_sessions,
  COUNT(*) AS total_sessions,
  ROUND(
    COUNTIF(session_source = '(direct)' AND session_medium = '(none)') / COUNT(*) * 100,
    1
  ) AS pct_direct
FROM (
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    session_source,
    session_medium,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id, ga_session_id ORDER BY event_timestamp) AS rn
  FROM `project.dataset.stg_ga4__events`
  WHERE event_date >= date_sub(current_date(), interval 3 day)
)
WHERE rn = 1
GROUP BY 1
HAVING pct_direct > 80;

-- ============================================================================
-- CHECK 6: Consent Mode v2 (2026 — Critical for EEA Traffic)
-- ============================================================================

SELECT
  event_date,
  COUNTIF(analytics_storage_consent = 'Granted') AS granted,
  COUNTIF(analytics_storage_consent = 'Denied') AS denied,
  COUNTIF(analytics_storage_consent IS NULL) AS unknown,
  ROUND(
    COUNTIF(analytics_storage_consent = 'Denied') / COUNT(*) * 100,
    1
  ) AS pct_denied
FROM `project.dataset.stg_ga4__events`
WHERE event_date >= date_sub(current_date(), interval 3 day)
GROUP BY 1
-- Alert if > 30% of traffic denies analytics_storage (investigate CMP/banner)
HAVING pct_denied > 30;

-- ============================================================================
-- CHECK 7: Registration Session Bug (user_id mid-session)
-- ============================================================================

-- Flag sessions where user_id appears after the session started.
-- These are at high risk of (direct)/(none) attribution in GA4 UI.
-- In BigQuery, session_traffic_source_last_click is more reliable, but
-- monitor this to catch GTM configuration issues.
SELECT
  event_date,
  COUNT(*) AS registration_sessions,
  COUNTIF(session_source = '(direct)' AND session_medium = '(none)') AS direct_after_registration
FROM `project.dataset.fct_ga4_sessions`
WHERE event_date >= date_sub(current_date(), interval 3 day)
  AND has_registration_mid_session = TRUE
GROUP BY 1
HAVING registration_sessions > 10;

/*
  If direct_after_registration is high, investigate your GTM setup:
  - Are you passing campaign_source/campaign_medium in dataLayer event pushes?
  - Are UTMs stripped when user navigates to login page?
  - See: https://gtm-gear.com/ga4-sessions-source-medium/
*/
