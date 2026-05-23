# Live to Ended Match Flow

Explains how a match transitions from active bronze capture to the ended view (`/api/ended/view`) and what must be true for it to appear there.

**Note: live match gold pages are out of scope. The pipeline processes only ended/inactive matches.**

---

## Pipeline Steps

### Step 1 — Bronze captures live snapshots
**Trigger:** `capture_cricket_inplay_snapshot` (Azure Function App timer) — every 5 seconds

While a match is in progress, the Function App calls BetsAPI and writes a snapshot bundle per event:
```
bronze/betsapi/inplay_snapshot/sport_id=3/event_id={id}/fi={fi}/snapshot_id={ts}/
    manifest.json
    api_live_market_stats.json
    api_live_market_odds.json
    api_event_view.json
    api_event_odds.json
    ...
```

**Deduplication:** before writing, the `api_live_market_stats` and `api_live_market_odds` payloads are hashed and compared against the previous snapshot's hashes (stored in `betsapi/control/snapshot_hash/event_id={id}.json`). If both hashes match and a snapshot was written within the last 5 minutes, the snapshot is skipped. A heartbeat snapshot is force-written every 5 minutes regardless. This reduces bronze volume by ~80–90% during quiet overs.

---

### Step 2 — Silver processes ended match snapshots
**Trigger:** ADF `pl_build_ended_match` Activity 1 — daily at 02:00 CET

Processes bronze snapshots into silver **only for matches that have been quiet for >60 minutes** (no new bronze snapshot in the last hour). Active live matches are never touched.

1. Scans bronze manifests and silver processed_snapshot markers (both scans run in parallel)
2. Filters to unprocessed snapshots for quiet event_ids only
3. Processes in parallel with 128 threads (IO-bound blob reads/writes)
4. For each snapshot: parses BetsAPI JSON → writes structured silver files + marker:
   ```
   silver/cricket/inplay/year={y}/month={m}/day={d}/hour={h}/event_id={id}/snapshot_id={sid}/
       match_state.json
       team_scores.json
       player_entries.json
       market_odds.json
       active_markets.json
       innings_snapshot.json
   silver/cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json
   ```

---

### Step 3 — Gold rebuilds innings tracker from silver
**Trigger:** ADF `pl_build_ended_match` Activity 2 — chained immediately after Activity 1 succeeds (same daily run)

Rebuilds `innings_1_from_silver.json` for every event where silver has data newer than the existing gold file. Reads from **silver only — no bronze access**.

**Stale detection:** compares the newest silver processed_snapshot marker timestamp per event against `last_modified` of `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json`. Rebuilds only where the gold file is missing or older.

Writes:
```
gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json
gold/cricket/innings_tracker/event_id={id}/innings_1.json
```

---

### Step 4 — Ended index picks it up
**Trigger:** `discover_cricket_ended` (Azure Function App timer) — every 1 hour (+20 min)

Scans gold for all `innings_1_from_silver.json` files, then for each:
- Skips if the league is excluded or the event is blocked
- Reads match name, score, batting order from the tracker file
- Writes to the ended index in **bronze**:
  ```
  bronze/cricket/ended/latest/index.json
  ```

`/api/ended/view` reads this index directly from bronze.

---

## Gate Conditions

A match appears in `/api/ended/view` only when **all** are true:

| Condition | Set by |
|---|---|
| `innings_1_from_silver.json` exists in gold | Step 3 (ADF gold rebuild) |
| League is in the allowed list | League filter toggle at `/api/mgmt/leagues/view` |
| Event ID not in blocked list | Admin block list |

---

## Why a Match Can Be Missing from Ended View

| Symptom | Root cause |
|---|---|
| `innings_1_from_silver.json` missing | Daily ADF run has not yet completed, or silver had no data for this match |
| Silver has no snapshots | Bronze capture was not running during the match, or all snapshots were deduped (no changes detected) |
| `innings_1_from_silver.json` exists but match not in ended index | `discover_cricket_ended` has not run yet (hourly) |
| League not allowed | League disabled in the league filter |

---

## Backfill (Manual)

If a match is missing from the ended view due to a processing gap, trigger `pl_backfill` in ADF Studio:

1. **Trigger `pl_backfill`** with `event_id=<id>` (or leave empty for all quiet matches)
2. Activity 1 processes silver for that match
3. Activity 2 rebuilds gold on success
4. `discover_cricket_ended` picks it up on the next hourly run

---

## Component Summary

| Component | Schedule | Runtime | Purpose |
|---|---|---|---|
| `capture_cricket_inplay_snapshot` | Every 5s (with dedup) | Azure Function App | Bronze — write live snapshots |
| ADF `pl_build_ended_match` Activity 1 | Daily 02:00 CET | Databricks | Silver — parse bronze for quiet matches |
| ADF `pl_build_ended_match` Activity 2 | Chained after Activity 1 | Databricks | Gold — rebuild innings tracker from silver |
| `discover_cricket_ended` | Every 1 hour (+20 min) | Azure Function App | Build ended match index in bronze |
| ADF `pl_backfill` | Manual only | Databricks | Silver + gold backfill for one match or all |
