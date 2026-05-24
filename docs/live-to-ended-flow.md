# Live to Ended Match Flow

How a match transitions from active bronze capture to appearing in `/api/ended/view`.

**Note: live match gold pages are out of scope. The pipeline processes ended/inactive matches only.**

---

## Step 1 — Bronze captures live snapshots

**Trigger:** `capture_cricket_inplay_snapshot` (Function App timer, every 5s)

While a match is in progress the Function App writes a snapshot bundle per event:

```
bronze/betsapi/inplay_snapshot/sport_id=3/event_id={id}/fi={fi}/snapshot_id={ts}/
    manifest.json
    api_live_market_stats.json
    api_live_market_odds.json
    api_event_view.json
    api_event_odds.json
    ... (8 files total)
```

**Dedup:** stats and odds payloads are hashed and compared to the previous snapshot. If both match and a snapshot was written within the last 5 minutes, the bundle is skipped. A heartbeat is force-written every 5 minutes regardless. Reduces bronze volume by ~80–90% during quiet overs.

---

## Step 2 — Silver processes ended match snapshots

**Trigger:** ADF `pl_build_ended_match` Activity 1, daily at 02:00 CET

Processes bronze snapshots into silver **only for matches quiet >60 minutes** (no new bronze snapshot in the last hour). Active live matches are never touched.

1. Scans bronze manifests and silver processed_snapshot markers in parallel
2. Filters to unprocessed snapshots for quiet event_ids only
3. Processes in parallel with 128 threads
4. For each snapshot: parses BetsAPI JSON → structured silver files + marker:

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

## Step 3 — Gold rebuilds innings tracker from silver

**Trigger:** ADF `pl_build_ended_match` Activity 2, chained immediately after Activity 1 succeeds

Rebuilds `innings_1_from_silver.json` for every event where silver has data newer than the existing gold file. Reads from **silver only — no bronze access**.

**Stale detection:** compares the newest silver processed_snapshot marker timestamp per event against `last_modified` of the gold file. Rebuilds only where the gold file is missing or older.

```
gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json
gold/cricket/innings_tracker/event_id={id}/innings_1.json
```

---

## Step 4 — Ended index is built from gold

**Trigger:** ADF `pl_build_ended_match` Activity 3, chained immediately after Activity 2 succeeds

Scans gold for all `innings_1_from_silver.json` files — presence of this file is the only gate. For each ended event: resolves `fi` from bronze path, reads match metadata from the tracker file, writes the ended index.

```
bronze/cricket/ended/latest/index.json
```

`/api/ended/view` reads this file directly.

**No league filter is applied here.** If a match reached gold, it already passed the league filter at bronze capture time. Filtering again caused silent gaps when tracker metadata was null.

---

## Gate Conditions

A match appears in `/api/ended/view` only when **all** are true:

| Condition | Set by |
|---|---|
| `innings_1_from_silver.json` exists in gold | Step 3 (ADF Activity 2) |
| Event ID not in blocked list | `gold/cricket/config/blocked_event_ids.json` |

---

## Why a Match May Be Missing

| Symptom | Cause |
|---|---|
| `innings_1_from_silver.json` missing | Daily ADF run not yet completed, or silver had no data |
| Silver has no snapshots | Bronze capture was not running, or all snapshots were deduped (no change detected) |
| Match in gold but not in ended index | `pl_build_ended_match` Activity 3 has not run yet — trigger manually or wait for next daily run |
| Gold tracker has null metadata | `_rebuild_innings_core` did not find metadata in silver snapshots — call `GET /api/mgmt/innings/{event_id}/rebuild` to backfill |

---

## Manual Backfill

If a match is missing from the ended view due to a processing gap:

1. Trigger `pl_backfill` in ADF Studio with `event_id=<id>` (or leave empty for all quiet matches)
2. Activity 1 processes silver for that match
3. Activity 2 rebuilds gold on success
4. Trigger `pl_build_ended_match` manually (or wait for the next daily run) to run Activity 3 and update the ended index

For metadata-only gaps (match is in gold but has null fields): call `GET /api/mgmt/innings/{event_id}/rebuild` — this backfills metadata from silver without a full ADF run.

---

## Component Summary

| Component | Schedule | Runtime | Purpose |
|---|---|---|---|
| `capture_cricket_inplay_snapshot` | Every 5s (with dedup) | Function App | Bronze — write live snapshots |
| `pl_build_ended_match` Activity 1 | Daily 02:00 CET | Databricks | Silver — parse bronze for quiet matches |
| `pl_build_ended_match` Activity 2 | Chained after Activity 1 | Databricks | Gold — rebuild innings tracker from silver |
| `pl_build_ended_match` Activity 3 | Chained after Activity 2 | Databricks | Bronze — write ended match index |
| `pl_backfill` | Manual only | Databricks | Silver + gold backfill for one match or all |
