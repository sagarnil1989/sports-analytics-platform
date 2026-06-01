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

#### Activities 4a & 4b — RunHypothesisInn2Over6 / RunHypothesisTimeoutWicket
*(both run in parallel if Activity 3 succeeds)*

Two hypothesis notebooks that run concurrently after Discover completes.

**RunHypothesisInn2Over6:** Scans all ended matches, finds the innings-2 over-6 snapshot for each, reads match-winner odds (`home_team_odds`/`away_team_odds`) and actual winner, writes results to gold.  
**Writes:** `gold/cricket/hypothesis/inn2_over6_favorite.json`

**RunHypothesisTimeoutWicket:** Detects strategic timeouts (game state unchanged >120s between snapshots), checks whether a wicket fell in the over that resumed, writes results to gold.  
**Writes:** `gold/cricket/hypothesis/timeout_wicket.json`

Both pages at `/api/hypothesis/…` are automatically up to date every morning.

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
    └─ Activity 1: RunSilverBuildEndedMatch       → silver files + markers
    └─ Activity 2: RunGoldBuildEndedMatch         → innings_1_from_silver.json     (if Activity 1 OK)
    └─ Activity 3: RunDiscoverCricketEnded        → ended index in bronze          (if Activity 2 OK)
    └─ Activity 4a: RunHypothesisInn2Over6        → hypothesis/inn2_over6.json     (if Activity 3 OK, parallel)
    └─ Activity 4b: RunHypothesisTimeoutWicket    → hypothesis/timeout_wicket.json (if Activity 3 OK, parallel)

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
| `/cricket-pipeline/ops/bronze_dedup_cleanup` | `infra/7.databricks/notebooks/bronze_dedup_cleanup.py` | Manual (one-time cleanup) |
| `/cricket-pipeline/analysis/snapshot_timeline` | `infra/7.databricks/notebooks/analysis_snapshot_timeline.py` | Manual (analysis) |
| `/cricket-pipeline/analysis/bronze_silver_counts` | `infra/7.databricks/notebooks/analysis_bronze_silver_counts.py` | Manual (analysis) |
| `/cricket-pipeline/analysis/match_data_explorer` | `infra/7.databricks/notebooks/analysis_match_data_explorer.py` | Manual (full match decode: batting/bowling scorecards, line movement, chase analysis, phase breakdown) |
| `/cricket-pipeline/hypothesis/inn2_over6` | `infra/7.databricks/notebooks/hypothesis_inn2_over6.py` | Manual — run after `pl_build_ended_match` to refresh hypothesis results |

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
