# GA4 BigQuery Incremental Refresh Patterns (Dataform Native)

I maintain GA4 BigQuery pipelines for clients on Google Cloud. **Dataform is my tool of choice** — it's native to BigQuery, compiles to pure SQL, and doesn't require a separate orchestrator for simple schedules.

One thing you learn fast: **running full refreshes on GA4 event data is prohibitively expensive**. A single backfill can cost €200+ in BigQuery scan charges. The 2023-era "merge with unique key" advice breaks down at scale because GA4 events have no true unique key, and BigQuery `MERGE` scans entire partitions.

This repo shows the **patterns I actually deploy in Dataform** — with compiled SQL, cost estimates, and 2026-native BigQuery features.

## The GA4 Data Problem

Google Analytics 4 exports to BigQuery in two streams:

| Export Type | Latency | Completeness | Table Suffix |
|-------------|---------|--------------|--------------|
| **Daily** | ~24h | Final (with 72h backfill window) | `events_YYYYMMDD` |
| **Intraday** | ~1h | Real-time, missing attribution fields | `events_intraday_YYYYMMDD` |

Challenges this creates:
1. **Late native events**: GA4 backfills up to 72 hours
2. **Measurement Protocol events**: Can be backdated 15-30+ days (invisible to a 3-day overwrite)
3. **Dual tables**: intraday is incomplete but fresh; daily is complete but stale
4. **No unique key**: `event_timestamp + user_pseudo_id` is not guaranteed unique
5. **Schema drift**: Google adds new event parameters without warning
6. **Consent Mode v2**: `privacy_info` struct tells you if EEA traffic granted consent (mandatory since March 2024)
7. **Registration session bug**: When `user_id` appears mid-session, GA4 UI resets attribution to `(direct) / (none)`

## Why Dataform?

| Feature | Dataform (GCP Native) | dbt (3rd Party) |
|---------|----------------------|-----------------|
| **Compilation** | Compiles to pure SQL in BigQuery | Requires dbt Cloud or self-hosted |
| **Scheduling** | Native Dataform Workflows (cron) | Requires Airflow/dbt Cloud |
| **Dependencies** | Auto-resolved via `ref()` in same project | Requires manifest parsing |
| **Cost** | Free (part of BigQuery) | dbt Cloud ~$100-300/seat |
| **Git** | Native Git integration in Console | External |
| **2026 syntax** | `config { type: "incremental" }` | Jinja `{{ config(...) }}` |

**I use Dataform for GCP-native clients; dbt when the client already has a dbt stack.** This repo is Dataform-first.

## 2026 vs 2023: What Changed

| Topic | 2023 Advice | 2026 Best Practice |
|-------|-------------|-------------------|
| **Session attribution** | Parse UTMs from `page_location` | Use `session_traffic_source_last_click` (native BQ field) |
| **Traffic source** | Extract from `event_params` | Use `collected_traffic_source` (native struct) |
| **EU compliance** | Ignored | Include `privacy_info` for Consent Mode v2 |
| **Cost guardrails** | Not mentioned | Set `maximum_bytes_billed` on every compilation |
| **Dedup key** | `user_pseudo_id + timestamp + event_name` | Add `event_bundle_sequence_id`; document non-uniqueness |
| **MP backfill** | Not addressed | Monthly 60-day deep backfill (Pattern 5) |

## The Patterns

### Pattern 1: Insert Overwrite (Recommended)

Replace the last 3 days of data on every run. Leave everything older untouched.

**Why this wins:**
- No `MERGE` (avoids full partition scans)
- No unique key needed (GA4 doesn't have one)
- Handles late events, intraday updates, and daily backfills automatically
- 10–20× cheaper than merge at 100M+ row scale
- Uses native `session_traffic_source_last_click` for reliable attribution

See: [`sql/01_insert_overwrite.sql`](sql/01_insert_overwrite.sql)

### Pattern 2: Date Checkpoint (Append-Only)

Track a high-water mark and only append new days. Fastest option when you know data never changes after initial export.

**Use when:**
- You have a separate "today" table for real-time dashboards
- Historical data is truly immutable (rare for GA4)

See: [`sql/02_date_checkpoint.sql`](sql/02_date_checkpoint.sql)

### Pattern 3: Two-Tier Architecture (Production-Grade)

Separate **source mart** (handles raw GA4 quirks) from **business models** (clean 1-day incrementals). This is what I deploy for clients.

**Benefits:**
- Raw GA4 complexity is contained in one place
- 20+ downstream models each increment on 1 day (cheap)
- Schema changes in GA4 only break one model
- Handles the "registration session" bug explicitly
- Consent-aware metrics for EEA compliance

See: [`sql/03_two_tier_pipeline.sql`](sql/03_two_tier_pipeline.sql)

### Pattern 4: Validation Guardrails

Seven checks I run after every refresh: row count delta, null rates, event distribution, duplicates, attribution gaps, Consent Mode status, and registration session flags.

See: [`sql/04_validation.sql`](sql/04_validation.sql)

### Pattern 5: Monthly Deep Backfill (Measurement Protocol / Offline Events)

The 3-day insert overwrite (Pattern 1) silently misses **Measurement Protocol events**, offline conversion imports, and CRM-synced data that is backdated 15-30+ days.

**Solution:** Keep the fast 3-day overwrite for daily freshness, but run a monthly job that rebuilds the last 60-90 days.

**Schedule:** 1st of each month at 03:00 UTC via Dataform Workflows.

**Cost impact:** One monthly run of 60 days ≈ €8-15.

See: [`sql/05_measurement_protocol_backfill.sql`](sql/05_measurement_protocol_backfill.sql)

## Dataform Project Structure

```
gcp-project/
├── definitions/
│   ├── 01_insert_overwrite.sqlx
│   ├── 02_date_checkpoint.sqlx
│   ├── 03_two_tier_pipeline.sqlx
│   ├── 04_validation.sqlx
│   └── 05_measurement_protocol_backfill.sqlx
├── includes/
│   └── constants.js          # project_id, dataset, lookback windows
├── workflow_settings.yaml     # defaultProject, defaultDataset, schedule
└── dataform.json              # legacy v1 config (optional)
```

**`workflow_settings.yaml`:**
```yaml
defaultProject: my-gcp-project
defaultDataset: analytics
defaultLocation: EU
```

**`includes/constants.js`:**
```javascript
const PROJECT_ID = "my-gcp-project";
const GA4_DATASET = "analytics_123456789";
const LOOKBACK_DAYS = 3;

module.exports = { PROJECT_ID, GA4_DATASET, LOOKBACK_DAYS };
```

## Cost Comparison

Pattern tested on a GA4 property with ~50M events/month:

| Pattern | Daily Cost | Monthly Cost | Notes |
|---------|-----------|--------------|-------|
| Full refresh | €8–12 | €250–360 | Prohibitively expensive |
| MERGE on unique key | €3–5 | €90–150 | Scans entire target partition |
| Insert overwrite (3-day) | €0.40–0.80 | €12–24 | **Recommended** |
| Date checkpoint | €0.10–0.30 | €3–9 | Fastest, but misses late data |

Costs are BigQuery scan charges only. Your mileage varies by event volume and column selection.

**Always set `maximum_bytes_billed`** in your Dataform workflow config. A runaway query can cost €50+ before you notice.

```yaml
# In workflow_settings.yaml or per-action config
bigquery:
  labels:
    cost_center: marketing_analytics
  additionalOptions:
    maximumBytesBilled: "107374182400"  # 100 GiB = ~€0.50
```

## When to Use What

| Scenario | Pattern | Why |
|----------|---------|-----|
| New project, < 1M events/month | Date checkpoint | Cheapest, simplest |
| Production, > 10M events/month | Insert overwrite | Cost control + handles late data |
| Multiple teams/dashboards | Two-tier | Isolates complexity, enables reuse |
| Real-time dashboards needed | Two-tier + today table | Near-real-time without destabilising history |
| CRM/offline conversions imported monthly | Two-tier + monthly backfill | Catches backdated MP events |
| EEA traffic (Consent Mode v2) | Any pattern + `privacy_info` | Compliance and accurate metrics |

## Files

```
sql/
  01_insert_overwrite.sql              # Pattern 1: Replace 3-day window
  02_date_checkpoint.sql               # Pattern 2: High-water mark append
  03_two_tier_pipeline.sql             # Pattern 3: Source mart + business models
  04_validation.sql                    # Data quality checks (7 guardrails)
  05_measurement_protocol_backfill.sql # Pattern 5: 60-day monthly rebuild
```

> **Note:** These are `.sql` files for readability on GitHub. In Dataform, rename to `.sqlx` and wrap the SQL in `config { ... }` / `pre_operations { ... }` blocks as shown in the file comments.

## License

MIT
