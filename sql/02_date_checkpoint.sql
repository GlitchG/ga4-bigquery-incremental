/*
  Pattern 2: Date Checkpoint (Append-Only)

  Track the maximum event_date already loaded, then append only new days.
  This is the cheapest pattern but requires that data never changes after
  initial export. With GA4's 72-hour backfill window, this assumption is
  often violated in practice.

  Use this only when:
  - You have a separate "today" table for real-time reporting
  - You accept that late events will be missed in historical data
  - Cost is the absolute priority
*/

-- In dbt:
-- {{ config(
--     materialized='incremental',
--     partition_by={'field': 'event_date', 'data_type': 'date'},
--     incremental_strategy='append'
-- ) }}

-- Pure SQL:
DECLARE date_checkpoint DATE DEFAULT (
  SELECT COALESCE(MAX(event_date), DATE_TRUNC(current_date(), YEAR))
  FROM `project.dataset.ga4_events_checkpoint`
);

INSERT INTO `project.dataset.ga4_events_checkpoint`
SELECT
  parse_date('%Y%m%d', event_date) AS event_date,
  user_pseudo_id,
  event_name,
  event_timestamp,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
FROM `project.analytics_123456789.events_*`
WHERE _table_suffix >= format_date('%Y%m%d', date_checkpoint)
  AND _table_suffix < format_date('%Y%m%d', current_date())  -- Exclude today (incomplete)
  -- Partition pruning: _table_suffix is a string, so we compare strings
  AND _table_suffix BETWEEN format_date('%Y%m%d', date_checkpoint)
                        AND format_date('%Y%m%d', date_sub(current_date(), interval 1 day));

/*
  CRITICAL: The WHERE clause must use _table_suffix directly with string
  literals. If you wrap it in a subquery (e.g. _table_suffix >= (SELECT...)),
  BigQuery cannot prune partitions and will scan the entire table.

  Bad (full scan):
    WHERE _table_suffix >= (SELECT MAX(...) FROM target_table)

  Good (partition pruning):
    WHERE _table_suffix >= '20240101'

  This is why we use a DECLARE statement: the variable is resolved before
  the query plan is built, allowing partition pruning to work.
*/
