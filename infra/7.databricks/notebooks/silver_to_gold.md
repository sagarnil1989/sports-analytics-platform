# silver_to_gold.py — Notebook Guide

## What it does

Reads processed silver data and rebuilds the gold innings tracker file for every ended match. Runs as the second step in both ADF pipelines, always after `bronze_to_silver` completes.

Only **ended matches** are ever written to gold. Live match data is intentionally not processed — `bronze_to_silver` only passes matches that have been quiet for ≥60 minutes, and `silver_to_gold` only rebuilds events that have a silver complete marker.

---

## When it runs

| Trigger | Pipeline | event_id | Behaviour |
|---|---|---|---|
| Daily 02:00 CET (automatic) | `pl_build_ended_match` | Not passed | Rebuilds gold for all matches whose silver is newer than their gold file |
| Manual | `pl_backfill` | Passed | Rebuilds gold for one specific match |
| Manual | `pl_backfill` | Not passed | Rebuilds gold for all stale matches |

---

## What it produces

For each processed match:

```
gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json
gold/cricket/innings_tracker/event_id={id}/innings_1.json
```

Both files contain the same ball-by-ball innings data reconstructed from silver. `innings_1_from_silver.json` signals to downstream notebooks (e.g. `discover_cricket_ended`) that this match has been fully rebuilt from silver.

---

## Step-by-step logic

### Step 1 — Install dependencies

```python
subprocess.run(["pip", "install", "--quiet",
    "azure-storage-blob", "azure-storage-file-datalake", "requests", "azure-functions"], ...)
```

### Step 2 — Set up environment

Reads `DATA_STORAGE_CONNECTION_STRING` and `SPORT_ID` from the Databricks secret scope `cricket-pipeline` and places them in environment variables (where `gold_rebuild.py` reads them).

### Step 3 — Read event_id parameter

If ADF passed an `event_id` widget, use it. If not (nightly run), `event_id` is `None` — all stale matches get rebuilt.

### Step 4 — Call gold_rebuild

```python
from gold_rebuild import gold_rebuild_ended_matches
gold_rebuild_ended_matches(event_id=event_id)
```

Delegates entirely to `gold_rebuild.gold_rebuild_ended_matches()`.

#### What `gold_rebuild_ended_matches(event_id)` does:

**Step 4a — Find matches to rebuild**

Scans `silver/cricket/control/complete/` — one file per event that `bronze_to_silver` fully processed. Each file is named `event_id={id}.json` and contains `completed_at_utc` and `snapshot_count`.

- If `event_id` is set: checks that event has a complete marker. If not, logs a warning and exits.
- If `event_id` is `None`: also scans gold for `innings_1_from_silver.json` timestamps. Rebuilds any event where the silver complete marker is newer than the gold file, or where no gold file exists yet.

**Step 4b — For each match to rebuild (`_rebuild_innings_core`)**

1. Lists all silver snapshot paths: `silver/cricket/inplay/event_id={id}/snapshot_id={id}/match_state.json` sorted oldest-first
2. Reads the most recent snapshots (last 50) to extract match metadata: team names, league, venue, final score
3. Fetches the authoritative final score from `bronze/betsapi/event_final/event_id={id}/event_view.json` (written by the daily `capture_ended_event_view` function). Falls back to a live BetsAPI call if the file doesn't exist yet
4. For each snapshot reads `match_state.json`, `team_scores.json`, `active_markets.json` and calls `extract_innings_snapshot()` to produce one tracker row (over, score, predicted total, odds)
5. Deduplicates rows by `(innings, over, score)` key — keeps the latest snapshot for each unique game state
6. Calculates outcome (`over` / `under` / `push`) by comparing the innings-1 predicted total against the actual final score
7. Writes `gold/cricket/innings_tracker/event_id={id}/innings_1.json` and `innings_1_from_silver.json` (identical content)
8. Also updates `silver/cricket/inplay/control/event_id={id}/innings_accumulator.json` with the rebuilt rows

**Step 4c — Staleness check**

Events already in gold whose `innings_1_from_silver.json` is newer than the silver complete marker are skipped — nothing has changed since the last rebuild.

---

## Notes

- This notebook was merged from two formerly separate notebooks (`gold_build_ended_match.py` for the nightly run, `gold_backfill.py` for manual backfill). The merged version handles both cases.
- All gold write logic lives in `gold_rebuild.py` (a DBFS library file managed by Terraform), not in this notebook.
- `gold_rebuild_ended_matches` previously scanned `silver/cricket/control/processed_snapshots/` (one file per snapshot). It now scans `silver/cricket/control/complete/` (one file per event) — much faster as the number of ended matches grows.
