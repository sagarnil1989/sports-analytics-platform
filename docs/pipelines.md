# Pipelines and Triggers Reference

## Function App — Active Timer Triggers

| Function | Schedule | What it does | Writes to |
|---|---|---|---|
| `discover_cricket_inplay` | Every 5s (on startup) | Calls `/v3/events/inplay`, updates live match control file | `bronze/betsapi/control/active_inplay_fi/latest.json` |
| `capture_cricket_inplay_snapshot` | Every 5s | For each live match: calls **9 BetsAPI endpoints** — `/v1/event/view`, `/v2/event/odds/summary`, `/v2/event/odds`, `/v1/event/history`, `/v1/event/stats_trend`, `/v1/bet365/event`, `/v1/bet365/event?stats=1`, `/v1/bet365/event?lineup=1`, `/v1/bet365/event?raw=1` — then writes snapshot bundle. Skipped if **stats** (`bet365/event?stats=1`) response body hash matches previous snapshot AND last write < 5 min ago (dedup). Odds (`bet365/event`) are excluded from dedup — they move every 5s even when game state is static. Heartbeat force-written every 5 min regardless. | `bronze/betsapi/inplay_snapshot/sport_id=3/event_id={eid}/fi={fi}/snapshot_id={ts}/` |
| `discover_cricket_upcoming` | Every 1hr (on the hour) | Calls `/v3/events/upcoming`, writes upcoming match control file. All leagues captured unfiltered here. | `bronze/betsapi/control/upcoming_cricket/latest.json` |
| `capture_cricket_prematch_odds` | Every 1hr (+10 min) | Reads upcoming control file, filters by allowed leagues, calls `/v4/bet365/prematch` for each match | `bronze/betsapi/prematch_snapshot/sport_id=3/event_id={eid}/fi={fi}/snapshot_id={ts}/` |

`discover_cricket_ended` is **Activity 3 in `pl_build_ended_match`** (a Databricks notebook). It makes **no BetsAPI calls at all** — there is no `/v3/events/ended` call anywhere in this platform. A match is considered ended simply when it stops appearing in the live inplay feed and a `innings_1_from_silver.json` gold tracker already exists for it. The notebook builds the ended match index by scanning gold blobs for those tracker files and bronze blobs for FI values — pure blob reads, no external API.

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

**Dedup rule:** before writing, the **response body** of `api_live_market_stats` is hashed and compared against the previous snapshot. The request wrapper (`called_at_utc`, `elapsed_ms`) is excluded from the hash — only the actual Bet365 content is compared. If the hash is identical and a snapshot was written within the last 5 minutes, the full bundle is skipped. A heartbeat is force-written every 5 minutes regardless. Reduces bronze volume significantly during between-ball quiet periods.

---

## ADF Pipelines

All pipelines use **Azure Batch Custom activities** (Batch version) or **Databricks notebook activities** (Databricks mirror). There is a `pl_build_ended_match` / `pl_build_ended_match_databricks` pair and a `pl_backfill` / `pl_backfill_databricks` pair. The Batch versions are primary; Databricks mirrors exist for comparison.

### Landing zone (watermark safety pattern)

Every pipeline run creates a per-run landing index in `landing/{run_id}/index.json`. This file lists the exact set of bronze snapshots to process so that downstream scripts only read the delta, not all of bronze.

**Watermark lifecycle:**
1. `index_new_snapshots` scans bronze from the last watermark cutoff and writes `landing/{run_id}/index.json`. The watermark is **not** advanced yet.
2. Downstream scripts (`bronze_to_silver`, `silver_to_gold`) read the landing index.
3. On success: `update_watermark` advances `landing/control/watermarks.json` to the scan start time.
4. On any failure: `cleanup_landing` (ADF native Delete activity, zero compute) deletes `landing/{run_id}/index.json`. Since the watermark was never advanced, the next run re-scans from the same cutoff — no data loss, no duplicate skip.

---

### pl_build_ended_match

**Schedule:** Daily at 02:00 CET (01:00 UTC) via `trigger_build_ended_match` (currently STOPPED — start manually).  
**Compute:** Azure Batch (`ls_azure_batch`).  
**Source:** `infra/4.azure-batch/scripts/`

Five sequential activities plus one failure-branch cleanup.

#### Activity 1 — index_new_snapshots
*(no dependency — runs first)*

Reads the watermark from `landing/control/watermarks.json` (full scan on first run). Lists all bronze `manifest.json` blobs modified since `watermark − 2h` (2h buffer catches late-arriving blobs). Writes `landing/{run_id}/index.json` containing every new snapshot's `event_id`, `snapshot_id`, `fi`, `manifest_path`.

Exits via `dbutils.notebook.exit(watermark_json)` so the Web Activity in the Databricks mirror can use the payload directly.

**Reads:** `bronze/betsapi/inplay_snapshot/sport_id=3/…`  
**Writes:** `landing/{run_id}/index.json`

#### Activity 2 — bronze_to_silver
*(runs if Activity 1 succeeds)*

Reads the landing index and parses each bronze snapshot into structured silver files. Runs with 128 parallel worker threads (IO-bound blob operations).

Key behaviours:
- **Quiet filter:** skips any event whose most recent snapshot is newer than 60 minutes (match still live).
- **Complete markers:** if every snapshot for an event succeeds, writes `silver/control/complete/event_id={eid}.json`. On the next run, events with a complete marker are skipped in O(1) listing (no per-snapshot recheck).
- **Incomplete captures:** snapshots that have only `manifest.json` + `lineage.json` (API payload files missing due to an early-June ingestion bug) are skipped gracefully and counted as `skipped_incomplete`, not failures. They still count toward the event's complete marker threshold.
- **Run log:** writes `gold/logs/pl_build_ended_match/{date}/{time}_bronze_to_silver.json` including `failed_snapshots[]` array with event_id, snapshot_id, error for every failure.

**Reads:** `landing/{run_id}/index.json`, `bronze/betsapi/inplay_snapshot/…`  
**Writes:** `silver/…`, `silver/control/complete/event_id={eid}.json`, `gold/logs/…`

#### Activity 3 — silver_to_gold
*(runs if Activity 2 succeeds)*

Reads silver files and builds the gold tracker (`innings_1_from_silver.json`) per event. Scoped by `EVENT_ID` parameter if set (backfill mode), otherwise processes all events with silver data newer than their gold file.

**Reads:** `silver/…`  
**Writes:** `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`

#### Activity 4 — update_watermark
*(runs if Activity 3 succeeds)*

Reads `landing/{run_id}/index.json` and advances `landing/control/watermarks.json` to the scan-start time recorded in the index. No-op when `EVENT_ID` is set (backfill mode — watermark is unchanged).

**Reads:** `landing/{run_id}/index.json`  
**Writes:** `landing/control/watermarks.json`

#### Activity 5 — cleanup_landing
*(runs if update_watermark FAILS or is SKIPPED — i.e., any upstream failure)*

ADF native Delete activity — no compute, no Batch/Databricks. Deletes `landing/{run_id}/index.json` so the next run re-scans from the unchanged watermark. Handles "blob not found" gracefully (no error if `index_new_snapshots` itself failed before writing the file).

**Deletes:** `landing/{run_id}/index.json`

---

### pl_build_ended_match_databricks

Mirror of `pl_build_ended_match` using Databricks notebook activities instead of Batch. Same activity sequence and watermark lifecycle. `update_watermark` is a Web Activity PUT to the blob REST API using ADF managed identity (no Databricks spin-up cost for a simple JSON write).

**Compute:** Databricks single-node `Standard_F4s_v2`, Runtime 15.4 LTS.  
**Source:** `infra/7.databricks/notebooks/`

---

### pl_backfill

**Schedule:** No automatic trigger — run manually in ADF Studio.  
**Parameter:** `event_id` (string, default empty).  
**Compute:** Azure Batch.

Pass a specific `event_id` to reprocess one match end-to-end. Leave empty to run over all bronze snapshots newer than the watermark (same as the daily run, useful for catch-up after an outage).

Four sequential activities plus cleanup — identical structure to `pl_build_ended_match` minus `discover_cricket_ended`.

#### Activity 1 — index_new_snapshots

Same as in `pl_build_ended_match`. If `EVENT_ID` is set, the scan filters bronze to only that event's manifests.

**Writes:** `landing/{run_id}/index.json`

#### Activity 2 — bronze_to_silver
*(runs if Activity 1 succeeds)*

Same script as `pl_build_ended_match`. If `EVENT_ID` is set, it deletes the existing complete marker for that event first (`silver/control/complete/event_id={eid}.json`) so the event is fully reprocessed even if it was previously marked done.

**Writes:** `silver/…`, complete markers, run log

#### Activity 3 — silver_to_gold
*(runs if Activity 2 succeeds)*

Same script as `pl_build_ended_match`. Scoped to the single `event_id` if set.

**Writes:** `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`

#### Activity 4 — update_watermark
*(runs if Activity 3 succeeds)*

**No-op when `EVENT_ID` is set** (exits 0 immediately). Watermark is only advanced on a full incremental run (empty `EVENT_ID`). This is intentional: backfilling one match should not shift the scan cutoff and cause other matches to be missed on the next daily run.

#### Activity 5 — cleanup_landing
*(runs if update_watermark FAILS or is SKIPPED)*

Same Delete activity as in `pl_build_ended_match`. Always runs on failure; runs after update_watermark is skipped when `EVENT_ID` is set and `silver_to_gold` failed.

---

### pl_backfill_databricks

Mirror of `pl_backfill` using Databricks. `update_watermark` uses a Databricks notebook (not a Web Activity) since backfill runs rarely and the compute cost is acceptable.

---

### pl_hypothesis / pl_hypothesis_databricks

**Schedule:** No automatic trigger — run manually.

Two hypothesis scripts run in parallel (no dependency between them). These were previously activities 4a/4b in `pl_build_ended_match` but were moved to a standalone pipeline so hypothesis analysis doesn't block or delay the daily ingestion pipeline.

- **hypothesis_inn2_over6** — Finds the innings-2 over-6 snapshot for each ended match, reads match-winner odds, determines actual winner, writes results.  
  **Writes:** `gold/cricket/hypothesis/inn2_over6_favorite.json`
- **hypothesis_timeout_wicket** — Detects strategic timeouts (>120s gap between snapshots), checks for wickets in the resumed over.  
  **Writes:** `gold/cricket/hypothesis/timeout_wicket.json`

---

## Pipeline Flow Diagram

```
Daily 01:00 UTC
  pl_build_ended_match  (trigger currently STOPPED)
    ├─ index_new_snapshots          → landing/{run_id}/index.json
    ├─ bronze_to_silver             → silver/ + complete markers + run log      (if index OK)
    ├─ silver_to_gold               → gold/cricket/innings_tracker/…            (if b2s OK)
    ├─ update_watermark             → landing/control/watermarks.json           (if s2g OK)
    └─ cleanup_landing [on failure] → deletes landing/{run_id}/index.json       (if update_watermark fails/skipped)

Manual
  pl_backfill  [event_id=optional]
    ├─ index_new_snapshots          → landing/{run_id}/index.json
    ├─ bronze_to_silver             → silver/ (deletes complete marker if event_id set)  (if index OK)
    ├─ silver_to_gold               → gold/cricket/innings_tracker/…            (if b2s OK)
    ├─ update_watermark             → no-op if event_id set, else advances watermark     (if s2g OK)
    └─ cleanup_landing [on failure] → deletes landing/{run_id}/index.json       (if update_watermark fails/skipped)

Manual
  pl_hypothesis / pl_hypothesis_databricks
    ├─ hypothesis_inn2_over6        → gold/cricket/hypothesis/inn2_over6_favorite.json   (parallel)
    └─ hypothesis_timeout_wicket    → gold/cricket/hypothesis/timeout_wicket.json        (parallel)
```

After running `pl_backfill` for a specific event_id, the ended index is not automatically refreshed. To include the backfilled match in `/api/ended`, run `pl_build_ended_match` manually (or wait for the next daily run — it will pick up gold files updated by the backfill).

---

## Batch Scripts

| Script | Used by | Source |
|---|---|---|
| `index_new_snapshots.py` | All Batch pipelines — Activity 1 | `infra/4.azure-batch/scripts/` |
| `bronze_to_silver.py` | All Batch pipelines — Activity 2 | `infra/4.azure-batch/scripts/` |
| `silver_to_gold.py` | All Batch pipelines — Activity 3 | `infra/4.azure-batch/scripts/` |
| `update_watermark.py` | All Batch pipelines — Activity 4 | `infra/4.azure-batch/scripts/` |
| `landing_index.py` | Shared lib (watermark read/write, index scan/read) | `infra/7.databricks/lib/` (uploaded to batch-scripts) |
| `snapshot_parser.py` | Shared lib (bronze→silver parse + write) | `infra/7.databricks/lib/` (uploaded to batch-scripts) |

## Databricks Notebooks

| Notebook (in Databricks) | Source file | Used by |
|---|---|---|
| `/cricket-pipeline/index_new_snapshots` | `infra/7.databricks/notebooks/index_new_snapshots.py` | `pl_build_ended_match_databricks`, `pl_backfill_databricks` — Activity 1 |
| `/cricket-pipeline/bronze_to_silver` | `infra/7.databricks/notebooks/bronze_to_silver.py` | `pl_build_ended_match_databricks`, `pl_backfill_databricks` — Activity 2 |
| `/cricket-pipeline/silver_to_gold` | `infra/7.databricks/notebooks/silver_to_gold.py` | `pl_build_ended_match_databricks`, `pl_backfill_databricks` — Activity 3 |
| `/cricket-pipeline/update_watermark` | `infra/7.databricks/notebooks/update_watermark.py` | `pl_backfill_databricks` — Activity 4 (daily uses Web Activity PUT instead) |
| `/cricket-pipeline/hypothesis_inn2_over6` | `infra/7.databricks/notebooks/hypothesis_inn2_over6.py` | `pl_hypothesis_databricks` |
| `/cricket-pipeline/hypothesis_timeout_wicket` | `infra/7.databricks/notebooks/hypothesis_timeout_wicket.py` | `pl_hypothesis_databricks` |
| `/cricket-pipeline/ops/bronze_dedup_cleanup` | `infra/7.databricks/notebooks/bronze_dedup_cleanup.py` | Manual (one-time cleanup) |
| `/cricket-pipeline/analysis/match_data_explorer` | `infra/7.databricks/notebooks/analysis_match_data_explorer.py` | Manual (full match decode: batting/bowling scorecards, line movement, chase analysis, phase breakdown) |

---

## Innings Tracker — Batting Team Detection

The live innings tracker (`extract_innings_snapshot` in `innings_tracker.py`) determines which team is batting using the following priority:

1. **Innings market name** *(primary, most reliable)*: The Bet365 O/U market for a full innings is always named `"{batting_team} 20 Overs Runs"`. Parsing the team name from this market name gives an unambiguous answer.
2. **First team_score row with a live score** *(fallback)*: Bet365 consistently puts the batting team's TE record before the fielding team's TE record in the API response. Used when no innings market is available (e.g., early snapshots before odds go live).

> **Why not PI?** The Bet365 `PI` field in TE records was initially assumed to mean "currently batting" (PI=1). It may also encode home/away or be inconsistently populated. The market-name approach is unambiguous and has no such ambiguity.

**Analytics page** (`/api/innings-tracker`): Only innings-1 rows are shown. Innings-2 rows (chasing team batting) are filtered out because the O/U prediction is for innings 1 only.

**Rebuild behaviour**: The admin rebuild (`/api/mgmt/innings/{event_id}/rebuild`) always re-derives the batting team from raw silver `team_scores.json` + `active_markets.json` — it does not use the cached `innings_snapshot.json` — so a rebuild will correct any wrong batting team stored from a previous code version.

---

## Hypothesis Notebooks

### hypothesis/inn2_over6

**Hypothesis:** During a chase, the team with lower match-winner odds after the end of the 6th over of the 2nd innings wins the match.

**Run:** Manually in Databricks at `/cricket-pipeline/hypothesis/inn2_over6`, after `pl_build_ended_match` has completed so gold tracker files are up to date.

**What it does:**
1. Lists all ended matches from gold `innings_tracker/` files
2. For each match: scans silver `innings_snapshot.json` files to find innings-2 snapshots
3. Identifies the first innings-2 snapshot at or after over 6.0 (6 complete overs)
4. Reads match-winner odds (`home_team_odds`, `away_team_odds`) from that snapshot
5. Determines actual winner: chasing team's final innings-2 score ≥ target → chasing team wins
6. Writes `gold/cricket/hypothesis/inn2_over6_favorite.json` with one row per match plus aggregate stats

**Display:** `/api/hypothesis/inn2-over6` on the display platform (also linked from Home → Hypothesis section)

**Source:** `src/functions/cricket_ingestion/hypothesis.py` → uploaded to DBFS as `hypothesis.py`

---

## One-Time Operations

### bronze_dedup_cleanup

**Purpose:** Retroactively removes duplicate bronze inplay snapshots caused by a bug in `_payload_hash()` that was hashing the full JSON wrapper (including `called_at_utc`) instead of just the response body. As a result, dedup never fired and every 5-second poll was written to storage.

**Run:** Manually in Databricks. Always run with `dry_run=true` first.

**Widgets:**
| Widget | Default | Description |
|---|---|---|
| `event_id` | *(blank)* | Specific match to clean; blank = all ended matches |
| `dry_run` | `true` | `true` = report only, no deletes |
| `max_workers` | `32` | Parallel download/delete threads |

**What it keeps:** First snapshot of each identical hash sequence, plus one snapshot per 5-minute heartbeat window — matching exactly what the fixed live capture would have written.

**Expected reduction:** ~50–70% of existing bronze inplay snapshots for matches captured before the dedup fix.

Python source files shared by notebooks live at `src/functions/cricket_ingestion/` and are uploaded to DBFS at `dbfs:/FileStore/cricket-pipeline/src/` via Terraform (`infra/7.databricks/main.tf`).

---

## ML Pipeline — pl_ml_retrain

**Schedule:** Weekly Sunday 03:00 UTC via `trigger_ml_retrain`.

Activity 1 runs first; Activities 2, 3, 4, 5, 6 all depend on Activity 1 succeeding and run in parallel.

#### Activity 1 — RunMLFeatureExtraction

Reads all ended-match `innings_1_from_silver.json` files from gold in parallel. Filters to T20 (max over 15–20). Flattens `rows[]` into one row per snapshot with engineered features. Excludes "push" outcomes.

**Writes:** `gold/cricket/ml_features/t20/features.parquet`

#### Activity 2 — RunMLModelTraining
*(only runs if Activity 1 succeeds)*

Loads the Parquet. Trains two XGBoost models using Group K-Fold CV (grouped by `event_id` to prevent data leakage):
- **Score predictor** (XGBoostRegressor) — predicts 1st innings final score from state at any over
- **Over/Under classifier** (XGBoostClassifier) — predicts whether actual score exceeds bookmaker's line

Logs experiments and registers both models in MLflow Model Registry.

**Reads:** `gold/cricket/ml_features/t20/features.parquet`  
**Writes:** `gold/cricket/ml_features/t20/model_accuracy.json` + MLflow Model Registry entries

#### Activity 3 — RunMLWinPredictor
*(only runs if Activity 1 succeeds, parallel with Activities 2, 4, 5, 6)*

Reads the gold tracker files and Parquet features. Trains XGBoost + Random Forest models per innings snapshot point, writes per-match predictions and model accuracy JSON.

**Writes:** `gold/cricket/ml_features/t20/win_predictor_results.json`

#### Activity 4 — RunInn1ScorePredictor
*(only runs if Activity 1 succeeds, parallel with Activities 2, 3, 5, 6)*

Predicts 1st innings final score from in-game state.

#### Activity 5 — RunHypothesisInn2Over6
*(only runs if Activity 1 succeeds, parallel with Activities 2, 3, 4, 6)*

Scans all ended gold tracker files (T20 only) for the inn2 over-6 favourite hypothesis. Re-runs weekly so new matches are included automatically.

**Writes:** `gold/cricket/hypothesis/inn2_over6_favorite.json`

#### Activity 6 — RunHypothesisTimeoutWicket
*(only runs if Activity 1 succeeds, parallel with Activities 2, 3, 4, 5)*

Scans all ended gold tracker files (T20 only) for strategic timeouts and checks for wickets in the resumed over.

**Writes:** `gold/cricket/hypothesis/timeout_wicket.json`

---

## Terraform Modules

| Module | What it manages |
|---|---|
| `infra/7.databricks/` | Databricks workspace, secret scope, DBFS source files, notebook resources |
| `infra/8.adf-config/` | ADF Key Vault linked service, Databricks linked service, pipelines, schedule triggers |
| `infra/9.ml/` | ML notebooks in Databricks (`/cricket-pipeline/ml/`), ADF `pl_ml_retrain` pipeline, weekly trigger |
