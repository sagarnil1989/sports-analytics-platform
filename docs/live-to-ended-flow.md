# Live to Ended Match Flow

Explains how a match transitions from the live view (`/api/matches/view`) to the ended
view (`/api/ended/view`) and what must be true for it to appear in each.

---

## Pipeline Steps

### Step 1 — Bronze captures live snapshots
**Timer:** `capture_cricket_inplay_snapshot` — every 5 seconds

While a match is in progress (`time_status=1`), the bronze pipeline calls BetsAPI and
writes a snapshot bundle per event:
```
bronze/betsapi/inplay_snapshot/sport_id=3/event_id={id}/fi={fi}/snapshot_id={ts}/
    manifest.json
    events_inplay.json
    bet365_event.json
    event_odds.json
    ...
```
The match also appears in the live gold index:
```
gold/cricket/matches/latest/index.json
```

---

### Step 2 — Silver processes snapshots
**Timer:** `parse_cricket_bronze_to_silver` — every 1 hour

For each unprocessed bronze snapshot, silver:
1. Loads the full marker set from `silver/cricket/control/processed_snapshots/` in one
   bulk listing (avoids per-blob network checks)
2. Filters bronze manifests to unprocessed only
3. Parses each snapshot and writes per-over innings data into the accumulator:
   ```
   silver/cricket/inplay/control/event_id={id}/innings_accumulator.json
   ```
4. Writes a marker so the snapshot is never re-processed:
   ```
   silver/cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json
   ```

The silver pipeline runs for up to 9 minutes per invocation (hard budget, inside the
10-minute Azure Functions timeout). If there is a large backlog it continues on the next
hourly run, making progress each time.

---

### Step 3 — Auto-rebuild detects the match has ended
**Timer:** `auto_rebuild_ended_innings_tracker` — every 10 minutes

Checks for matches that meet **all** of the following:
- `innings_1.json` exists in gold (match was captured live) **OR** a silver accumulator exists (gold missed the match but silver processed some snapshots)
- `innings_1_from_silver.json` does **not** yet exist
- Event ID is **not** in the current live index
- File has not been modified for **more than 1 hour** (match is quiet)
- League is in the allowed list

For each qualifying match (capped at 2 per run to stay within the timeout), calls
`_rebuild_innings_core()` which:
1. Reads all silver accumulator data for the event
2. Builds the full innings timeline
3. Writes the gold innings tracker file:
   ```
   gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json
   ```

---

### Step 4 — Ended index picks it up
**Timer:** `discover_cricket_ended` — every 5 minutes

Scans gold for all `innings_1_from_silver.json` files, then for each:
- Skips if the event is still in the live index
- Skips if the league is excluded or the event is blocked
- Reads match name, score, batting order from the tracker file
- Corrects score and name ordering so 1st-innings team always appears first
- Adds to the ended index:
  ```
  gold/cricket/ended/latest/index.json
  ```

`/api/ended/view` reads this index directly.

---

## Gate Conditions

A match appears in `/api/ended/view` only when **all** are true:

| Condition | Set by |
|---|---|
| `innings_1_from_silver.json` exists in gold | Step 3 (auto-rebuild) |
| Event ID not in live index | BetsAPI — match must have ended |
| League is in the allowed list | League filter toggle |
| Event ID not in blocked list | Admin block list |

---

## Why a Match Can Be Missing from Ended View

| Symptom | Root cause |
|---|---|
| `innings_1_from_silver.json` missing | Silver pipeline never processed the bronze snapshots (gap in silver run) |
| Silver markers missing for early snapshots | Silver pipeline was not running during that window (Consumption plan cold start, or overwhelmed by concurrent matches) |
| Match still in live index | BetsAPI still reporting it as live — `discover_cricket_ended` skips it |
| League not allowed | League disabled in the league filter |

---

## Silver Pipeline Backfill Behaviour (as of May 2026)

Old snapshots that were missed during a live match are automatically picked up on
subsequent hourly silver runs — no manual trigger needed. The pipeline:

1. Builds a set of all processed keys in memory (one bulk listing)
2. Diffs against all bronze manifests to find unprocessed ones
3. Processes newest-first within the 9-minute budget
4. Stops cleanly and resumes on the next hourly run

A match with a large backlog (e.g. 2,000+ unprocessed snapshots) will be fully caught
up within 2–3 hourly runs. Once silver is complete, the auto-rebuild timer picks it up
within 10 minutes and it appears in the ended view within 15 minutes after that.

---

## Timers Summary

| Timer | Schedule | Purpose |
|---|---|---|
| `capture_cricket_inplay_snapshot` | Every 5s | Bronze — write live snapshots |
| `parse_cricket_bronze_to_silver` | Every 1 hour | Silver — process snapshots |
| `build_cricket_gold_match_pages` | Every 10s | Gold — write live innings_1.json |
| `auto_rebuild_ended_innings_tracker` | Every 10 min | Rebuild innings_1_from_silver.json for ended matches |
| `discover_cricket_ended` | Every 5 min | Build ended match index |
