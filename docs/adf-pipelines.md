# ADF Pipelines — What They Do and Why

There are **2 pipelines** in ADF, each with two sequential Databricks notebook activities. Silver always runs first; gold only runs if silver succeeds.

Live match processing is out of scope — all pipelines operate on **ended or inactive matches** only.

---

## pl_build_ended_match

**Schedule:** Daily at 02:00 CET (01:00 UTC).

### Activity 1 — RunSilverBuildEndedMatch

Scans bronze snapshots and parses them into structured silver files, but only for matches that have had no new bronze snapshot in the last 60 minutes. Active live matches are never touched.

**How it knows what to process:**
No timestamp watermark. Each processed bronze snapshot leaves a marker file at `silver/cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json`. On each run the notebook lists all bronze manifests, lists all existing markers, and processes only the difference — for event_ids that are "quiet" (no new snapshot in >60 min).

**Reads from:** `bronze/betsapi/inplay_snapshot/…`
**Writes to:** `silver/cricket/inplay/year=…/event_id=…/snapshot_id=…/*.json` + processed_snapshot markers

### Activity 2 — RunGoldBuildEndedMatch
*(only runs if Activity 1 succeeds)*

Rebuilds `innings_1_from_silver.json` for every match where silver has newer data than the existing gold file. Reads from silver only — no bronze access.

**How it detects stale gold:**
Scans silver processed_snapshot markers for the newest marker timestamp per event_id, then compares against the `last_modified` of `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`. Rebuilds only where the gold file is missing or older than the newest silver marker.

**Reads from:** `silver/cricket/inplay/…`, `silver/cricket/control/processed_snapshots/`
**Writes to:** `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`

---

## pl_backfill

**Schedule:** No automatic trigger. Triggered manually in ADF Studio.

**Parameters:**
| Parameter | Default | Meaning |
|---|---|---|
| `event_id` | *(empty)* | Pass a specific match ID to process one match. Leave empty for full backfill. |

Both activities receive the same `event_id` parameter.

### Activity 1 — RunSilverBackfill

If `event_id` is provided: processes that one match once and exits.
If empty: loops every 10 seconds for up to 4 hours, continuously draining the backlog.

**Reads from:** `bronze/betsapi/inplay_snapshot/…`
**Writes to:** `silver/cricket/inplay/…` + processed_snapshot markers

### Activity 2 — RunGoldBackfill
*(only runs if Activity 1 succeeds)*

If `event_id` is provided: rebuilds gold for that one match.
If empty: rebuilds gold for all events where silver is newer than the existing gold file.

**Reads from:** `silver/cricket/inplay/…`, `silver/cricket/control/processed_snapshots/`
**Writes to:** `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json`

---

## Pipeline flow

```
Daily (02:00 CET)
  pl_build_ended_match
    └─ Activity 1: RunSilverBuildEndedMatch   ──► silver files + markers
    └─ Activity 2: RunGoldBuildEndedMatch     ──► innings_1_from_silver.json
         (only if Activity 1 succeeded)

Manual backfill
  pl_backfill  [event_id=optional]
    └─ Activity 1: RunSilverBackfill          ──► silver files + markers
    └─ Activity 2: RunGoldBackfill            ──► innings_1_from_silver.json
         (only if Activity 1 succeeded)
```

---

## Summary Table

| Pipeline | Trigger | Activity 1 | Activity 2 |
|---|---|---|---|
| `pl_build_ended_match` | Daily 02:00 CET | Silver: parse bronze → silver for quiet matches | Gold: rebuild innings from silver (on Activity 1 success) |
| `pl_backfill` | Manual only | Silver: backfill one match or loop 4 hr | Gold: rebuild gold for same scope (on Activity 1 success) |
