# GA4 BigQuery Incremental Refresh Patterns

I maintain GA4 BigQuery pipelines for clients who spend €50K–€500K/month on ads. One thing you learn fast: **running full refreshes on GA4 event data is prohibitively expensive**. A single backfill can cost €200+ in BigQuery scan charges. The 2023-era "merge with unique key" advice breaks down at scale because GA4 events have no true unique key, and BigQuery `MERGE` scans entire partitions.

This repo shows the **three patterns I actually use in production** — with cost estimates, trade-offs, and when to use each.

## The GA4 Data Problem

Google Analytics 4 exports to BigQuery in two streams:

| Export Type | Latency | Completeness | Table Suffix |
|-------------|---------|--------------|--------------|
| **Daily** | ~24h | Final (with 72h backfill window) | `events_YYYYMMDD` |
| **Intraday** | ~1h | Real-time, missing attribution fields | `events_intraday_YYYYMMDD` |

Challenges this creates:
1. **Late events**: GA4 can backfill events up to 72 hours late
2. **Dual tables**: intraday data is incomplete but fresh; daily data is complete but stale
3. **No unique key**: `event_timestamp + user_pseudo_id` is not guaranteed unique (batch uploads, retries)
4. **Schema drift**: Google adds new event parameters without warning

## The Patterns

### Pattern 1: Insert Overwrite (Recommended for 2026)

Replace the last 3 days of data on every run. Leave everything older untouched.

**Why this wins:**
- No `MERGE` (avoids full partition scans)
- No unique key needed (GA4 doesn't have one)
- Handles late events, intraday updates, and daily backfills automatically
- 10–20× cheaper than merge at 100M+ row scale

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

See: [`sql/03_two_tier_pipeline.sql`](sql/03_two_tier_pipeline.sql)

## Cost Comparison

Pattern tested on a GA4 property with ~50M events/month:

| Pattern | Daily Cost | Monthly Cost | Notes |
|---------|-----------|--------------|-------|
| Full refresh | €8–12 | €250–360 | Prohibitively expensive |
| MERGE on unique key | €3–5 | €90–150 | Scans entire target partition |
| Insert overwrite (3-day) | €0.40–0.80 | €12–24 | **Recommended** |
| Date checkpoint | €0.10–0.30 | €3–9 | Fastest, but misses late data |

Costs are BigQuery scan charges only. Your mileage varies by event volume and column selection.

## Validation

Every incremental pipeline needs guardrails. I run these checks after each refresh:

```sql
-- 1. Row count delta vs previous day
-- 2. Null rate on critical fields (user_pseudo_id, event_timestamp)
-- 3. Event distribution by event_name (catch tracking outages)
-- 4. Session count vs GA4 UI (within 5% tolerance)
```

See: [`sql/04_validation.sql`](sql/04_validation.sql)

## When to Use What

| Scenario | Pattern | Why |
|----------|---------|-----|
| New project, < 1M events/month | Date checkpoint | Cheapest, simplest |
| Production, > 10M events/month | Insert overwrite | Cost control + handles late data |
| Multiple teams/dashboards | Two-tier | Isolates complexity, enables reuse |
| Real-time dashboards needed | Two-tier + today table | Near-real-time without destabilising history |

## Files

```
sql/
  01_insert_overwrite.sql      # Pattern 1: Replace 3-day window
  02_date_checkpoint.sql       # Pattern 2: High-water mark append
  03_two_tier_pipeline.sql     # Pattern 3: Source mart + business models
  04_validation.sql            # Data quality checks
```

## License

MIT
