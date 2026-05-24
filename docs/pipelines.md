# Pipelines and Triggers Reference

## Function App — Active Timer Triggers

| Function | Schedule | What it does | Writes to |
|---|---|---|---|
| `discover_cricket_inplay` | Every 5s (on startup) | Calls `/v3/events/inplay`, updates live match control file | `bronze/betsapi/control/active_inplay_fi/latest.json` |
| `capture_cricket_inplay_snapshot` | Every 5s | For each live match: calls 8 BetsAPI endpoints, writes snapshot bundle. Skipped if stats+odds **response body** hashes match previous snapshot AND last write < 5 min ago (dedup). Heartbeat force-written every 5 min regardless. | `bronze/betsapi/inplay_snapshot/sport_id=3/event_id={eid}/fi={fi}/snapshot_id={ts}/` |
| `discover_cricket_upcoming` | Every 1hr (on the hour) | Calls `/v3/events/upcoming`, writes upcoming match control file. All leagues captured unfiltered here. | `bronze/betsapi/control/upcoming_cricket/latest.json` |
| `capture_cricket_prematch_odds` | Every 1hr (+10 min) | Reads upcoming control file, filters by allowed leagues, calls `/v4/bet365/prematch` for each match | `bronze/betsapi/prematch_snapshot/sport_id=3/event_id={eid}/fi={fi}/snapshot_id={ts}/` |

`discover_cricket_ended` was previously a Function App timer. It is now **Activity 3 in `pl_build_ended_match`** — moved to ADF to remove the 10-minute timeout ceiling.

---

## Inplay Snapshot Files

Each inplay snapshot bundle contains:

| File | Source API |
|---|---|
| `manifest.json` | Metadata: event_id, fi, snapshot time, paths |
| `api_inplay_event_list.json` | `/v3/events/inplay` |
| `api_event_view.json` | `/v1/event/view` |
| `api_event_odds_summary.json` | `/v2/event/odds/summary` |
| `api_event_odds.json` | `/v2/event/odds` |
| `api_event_history.json` | `/v1/event/history` |
| `api_event_stats_trend.json` | `/v1/event/stats_trend` |
| `api_live_market_odds.json` | `/v1/bet365/event?FI=…` |
| `api_live_market_stats.json` | `/v1/bet365/event?FI=…&stats=1` |
| `api_live_market_lineup.json` | `/v1/bet365/event?FI=…&lineup=1` |
| `api_live_market_raw.json` | `/v1/bet365/event?FI=…&raw=1` |
| `lineage.json` | API call log |

**Dedup rule:** before writing, the **response body** of `api_live_market_stats` and `api_live_market_odds` is hashed and compared against the previous snapshot. The request wrapper (`called_at_utc`, `elapsed_ms`) is excluded from the hash — only the actual Bet365 content is compared. If both bodies are identical and a snapshot was written within the last 5 minutes, the full bundle is skipped. A heartbeat is force-written every 5 minutes regardless. Reduces bronze volume significantly during between-ball quiet periods.

---

## ADF Pipelines

### pl_build_ended_match

**Schedule:** Daily at 02:00 CET (01:00 UTC) via `trigger_build_ended_match`.

Three sequential activities — each depends on the previous succeeding.

#### Activity 1 — RunSilverBuildEndedMatch

Scans bronze inplay snapshots and parses them into structured silver files. Only processes matches that have had **no new bronze snapshot in the last 60 minutes** (quiet = ended or inactive). Active live matches are never touched.

**How it avoids reprocessing:** Each processed snapshot leaves a marker at `silver/cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json`. On each run the notebook lists all bronze manifests and all existing markers in parallel, then processes only the difference for quiet event_ids.

**Reads:** `bronze/betsapi/inplay_snapshot/…`  
**Writes:** `silver/cricket/inplay/year=…/event_id=…/snapshot_id=…/*.json` + processed_snapshot markers

#### Activity 2 — RunGoldBuildEndedMatch
*(only runs if Activity 1 succeeds)*

Rebuilds `innings_1_from_silver.json` for every event where silver has data newer than the existing gold file. Reads from **silver only — no bronze access**.

**Stale detection:** compares the newest silver marker timestamp per event against `last_modified` of `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`. Rebuilds only where the gold file is missing or older.

**Reads:** `silver/cricket/inplay/…`, `silver/cricket/control/processed_snapshots/`  
**Writes:** `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`

#### Activity 3 — RunDiscoverCricketEnded
*(only runs if Activity 2 succeeds)*

Scans gold for all `innings_1_from_silver.json` files and writes the ended match index. No API calls — all data from gold/bronze blobs only.

Steps:
1. Load blocked event IDs from `gold/cricket/config/blocked_event_ids.json`
2. Load currently-live event IDs from `gold/cricket/matches/latest/index.json` (excluded from ended)
3. Scan gold for all `innings_1_from_silver.json` files — presence = match is ended
4. For each ended event: resolve `fi` from bronze snapshot path
5. Read match metadata (name, league, score, batting order) from gold tracker file
6. Write sorted ended index to `bronze/cricket/ended/latest/index.json`

**Note:** League filtering is intentionally **not applied** here. If a match reached gold, it already passed the league filter at bronze capture time. Filtering again caused silent gaps when tracker metadata was null.

**Reads:** `gold/cricket/innings_tracker/…`, `gold/cricket/config/…`, `bronze/betsapi/…`  
**Writes:** `bronze/cricket/ended/latest/index.json`

---

### pl_backfill

**Schedule:** No automatic trigger — run manually in ADF Studio.

**Parameter:** `event_id` (optional). Pass a specific event ID to process one match; leave empty to process all quiet matches (loops for up to 4 hours).

#### Activity 1 — RunSilverBackfill

Same logic as `RunSilverBuildEndedMatch` but accepts `event_id` parameter.

#### Activity 2 — RunGoldBackfill
*(only runs if Activity 1 succeeds)*

Same logic as `RunGoldBuildEndedMatch` but scoped to the provided `event_id` (or all stale events if empty).

---

## Pipeline Flow Diagram

```
Daily 02:00 CET
  pl_build_ended_match
    └─ Activity 1: RunSilverBuildEndedMatch   → silver files + markers
    └─ Activity 2: RunGoldBuildEndedMatch     → innings_1_from_silver.json  (if Activity 1 OK)
    └─ Activity 3: RunDiscoverCricketEnded    → ended index in bronze       (if Activity 2 OK)

Manual
  pl_backfill  [event_id=optional]
    └─ Activity 1: RunSilverBackfill          → silver files + markers
    └─ Activity 2: RunGoldBackfill            → innings_1_from_silver.json  (if Activity 1 OK)
```

After `pl_backfill` completes, trigger `pl_build_ended_match` manually to run Activity 3 and refresh the ended index, or wait for the next daily run.

---

## Databricks Notebooks

| Notebook (in Databricks) | Source file | Used by |
|---|---|---|
| `/cricket-pipeline/silver_build_ended_match` | `infra/7.databricks/notebooks/silver_build_ended_match.py` | `pl_build_ended_match` Activity 1 |
| `/cricket-pipeline/gold_build_ended_match` | `infra/7.databricks/notebooks/gold_build_ended_match.py` | `pl_build_ended_match` Activity 2 |
| `/cricket-pipeline/discover_cricket_ended` | `infra/7.databricks/notebooks/discover_cricket_ended.py` | `pl_build_ended_match` Activity 3 |
| `/cricket-pipeline/silver_backfill` | `infra/7.databricks/notebooks/silver_backfill.py` | `pl_backfill` Activity 1 |
| `/cricket-pipeline/gold_backfill` | `infra/7.databricks/notebooks/gold_backfill.py` | `pl_backfill` Activity 2 |

Python source files shared by notebooks live at `src/functions/cricket_ingestion/` and are uploaded to DBFS at `dbfs:/FileStore/cricket-pipeline/src/` via Terraform (`infra/7.databricks/main.tf`).

---

## Terraform Modules

| Module | What it manages |
|---|---|
| `infra/7.databricks/` | Databricks workspace, secret scope, DBFS source files, notebook resources |
| `infra/8.adf-config/` | ADF Key Vault linked service, Databricks linked service, pipelines, schedule triggers |
