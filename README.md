# GA4 BigQuery Incremental Refresh Patterns (Dataform Native)

Production-grade Dataform project for GA4 BigQuery incremental refresh. Compiles and runs with `dataform compile && dataform run`.

**Dataform is my tool of choice** for GCP-native clients — it compiles to pure SQL in BigQuery, requires no separate orchestrator, and has no per-seat cost. I use dbt when the client already has a dbt stack.

## The GA4 Data Problem

Google Analytics 4 exports to BigQuery in two streams:

| Export Type | Latency | Completeness | Table Suffix |
|-------------|---------|--------------|--------------|
| **Daily** | ~24h | Final (with 72h backfill window) | `events_YYYYMMDD` |
| **Intraday** | ~1h | Real-time, missing attribution fields | `events_intraday_YYYYMMDD` |

Challenges: late native events (72h), Measurement Protocol events (backdated 15-30+ days), no unique key, schema drift, Consent Mode v2, and the registration session attribution bug.

## Project Structure

```
definitions/
├── sources/
│   ├── ga4_events.sqlx                    -- External daily export declaration
│   └── ga4_events_intraday.sqlx           -- External intraday export declaration
├── 01_insert_overwrite.sqlx              -- Pattern 1: 3-day insert overwrite (recommended)
├── 02_date_checkpoint.sqlx               -- Pattern 2: Append-only date checkpoint
├── 03a_two_tier_source_mart.sqlx         -- Pattern 3 Tier 1: Source mart
├── 03b_two_tier_sessions.sqlx            -- Pattern 3 Tier 2: Example business model
├── 04_validation.sqlx                    -- Pattern 4: Data quality guardrails
└── 05_measurement_protocol_backfill.sqlx -- Pattern 5: Monthly 60-day deep backfill
includes/
└── constants.js                           -- Centralised vars (project, dataset, lookback)
workflow_settings.yaml                     -- dataformCoreVersion, defaultProject, vars
```

## Running the Project

```bash
# 1. Set your GCP project and GA4 dataset in workflow_settings.yaml
# 2. Compile
npm install -g @dataform/cli
dataform compile

# 3. Run all patterns
#    NOTE: Patterns 1-3 all write to analytics.stg_ga4_events_* tables.
#    In production, pick ONE pattern — they are mutually exclusive strategies.
dataform run

# 4. Run a specific pattern
dataform run --actions stg_ga4_events_insert_overwrite
```

## Configuration

Edit `workflow_settings.yaml`:

```yaml
dataformCoreVersion: "3.0.20"
defaultProject: your-gcp-project
defaultLocation: EU
defaultDataset: analytics
vars:
  ga4_project: "bigquery-public-data"        # or your project
  ga4_dataset: "ga4_obfuscated_sample_ecommerce"  # or your dataset
  lookback_days: 3
  mp_lookback_days: 60
```

## The Patterns

### Pattern 1: Insert Overwrite (Recommended)
Replace the last 3 days of data on every run. No MERGE, no unique key needed. 10–20× cheaper than MERGE at 100M+ row scale. Uses native `session_traffic_source_last_click` for reliable attribution and includes `privacy_info` for Consent Mode v2.

### Pattern 2: Date Checkpoint (Append-Only)
Track a high-water mark and only append new days. Fastest option when data is truly immutable. Rarely suitable for GA4 due to 72h backfills.

### Pattern 3: Two-Tier Architecture (Production-Grade)
**Tier 1 (Source Mart):** Handle GA4 quirks once — union daily + intraday, deduplicate, 3-day overwrite.
**Tier 2 (Business Models):** Clean 1-day incrementals on stable data. Session models, conversion funnels, LTV — each increments on 1 day (cheap).

### Pattern 4: Validation Guardrails
Seven checks: row count delta, null rates, event distribution, duplicates, attribution gaps, Consent Mode status, and cost guardrails. Run as `operations` after every refresh.

### Pattern 5: Monthly Deep Backfill
Rebuild the last 60 days to catch Measurement Protocol events, offline conversions, and CRM-synced data that the 3-day overwrite misses. Schedule via Dataform Workflows on the 1st of each month.

## Cost Comparison (50M events/month)

| Pattern | Daily Cost | Monthly Cost | Notes |
|---------|-----------|--------------|-------|
| Full refresh | €8–12 | €250–360 | Prohibitively expensive |
| MERGE on unique key | €3–5 | €90–150 | Scans entire target partition |
| Insert overwrite (3-day) | €0.40–0.80 | €12–24 | **Recommended** |
| Date checkpoint | €0.10–0.30 | €3–9 | Fastest, but misses late data |

## When to Use What

| Scenario | Pattern | Why |
|----------|---------|-----|
| New project, < 1M events/month | Date checkpoint | Cheapest, simplest |
| Production, > 10M events/month | Insert overwrite | Cost control + handles late data |
| Multiple teams/dashboards | Two-tier | Isolates complexity, enables reuse |
| Real-time dashboards needed | Two-tier + today table | Near-real-time without destabilising history |
| CRM/offline conversions imported monthly | Two-tier + monthly backfill | Catches backdated MP events |
| EEA traffic (Consent Mode v2) | Any pattern + `privacy_info` | Compliance and accurate metrics |

## 2026 vs 2023: What Changed

| Topic | 2023 Advice | 2026 Best Practice |
|-------|-------------|-------------------|
| **Session attribution** | Parse UTMs from `page_location` | Use `session_traffic_source_last_click` (native BQ field) |
| **Traffic source** | Extract from `event_params` | Use `collected_traffic_source` (native struct) |
| **EU compliance** | Ignored | Include `privacy_info` for Consent Mode v2 |
| **Cost guardrails** | Not mentioned | Set `maximum_bytes_billed` on every compilation |
| **Dedup key** | `user_pseudo_id + timestamp + event_name` | Add `event_bundle_sequence_id`; document non-uniqueness |
| **MP backfill** | Not addressed | Monthly 60-day deep backfill (Pattern 5) |

## Related

- [ga4-attribution-models](https://github.com/GlitchG/ga4-attribution-models) — multi-touch attribution in BigQuery/Dataform
- [bigquery-meridian-mmm](https://github.com/GlitchG/bigquery-meridian-mmm) — Bayesian marketing mix modelling
- [simple-marketing-mix-model](https://github.com/GlitchG/simple-marketing-mix-model) — lightweight MMM in Python
- [landing-page-ab-testing](https://github.com/GlitchG/landing-page-ab-testing) — GA4 A/B testing in BigQuery

MIT
