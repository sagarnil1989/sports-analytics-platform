# bronze_to_silver.py — Notebook Guide

## What this notebook does

Reads raw API snapshots from the **bronze** layer and writes structured cricket data into the **silver** layer. It is the single notebook that handles silver processing for both the nightly routine and manual catch-up runs.

---

## Two ways it gets triggered

### 1. Daily automatic run — via `pl_build_ended_match` (02:00 CET)
No `event_id` is passed. The notebook finds every match that has had no new snapshot for over 60 minutes (i.e. the match has ended), and processes all their unprocessed snapshots.

### 2. Manual run — via `pl_backfill`
You pass a specific `event_id`. The notebook deletes the complete marker for that event (forcing a full reprocess), skips the 60-minute quiet check, and processes all snapshots for that match from scratch.

---

## What it produces

For every snapshot it processes, it writes these files to the **silver** container:

```
silver/cricket/inplay/
  event_id={id}/
    snapshot_id={id}/
      match_state.json      ← innings, over, ball, time status, score
      team_scores.json      ← batting/bowling figures, run rate, required rate
      market_odds.json      ← Bet365 market list (winner, next wicket, over runs…)
      player_entries.json   ← individual player records
      active_markets.json   ← currently open markets
      innings_snapshot.json ← one row for the innings tracker timeline

  control/
    event_id={id}/
      innings_accumulator.json   ← full ball-by-ball timeline built up over the match
      last_known_markets.json    ← most recent market snapshot

cricket/control/complete/
  event_id={id}.json  ← written once all snapshots for an event are successfully processed
```

---

## Step-by-step logic

### Step 1 — Install and connect
Installs `azure-storage-blob` and reads `DATA_STORAGE_CONNECTION_STRING` + `SPORT_ID` from Databricks secrets.

### Step 2 — Read the `event_id` parameter
If a value is passed (manual run), only that match will be processed.
If empty (nightly run), all quiet matches will be processed.

### Step 3 — Scan bronze and silver in parallel
Two scans run at the same time to save time:

- **Bronze scan** — finds every `manifest.json` under `bronze/betsapi/inplay_snapshot/sport_id=3/`. Each manifest represents one captured snapshot. Records the `event_id`, `snapshot_id`, `fi`, and `last_modified` timestamp for each.
- **Silver scan** — lists every file under `silver/cricket/control/complete/`. Each file represents one event that has been fully processed. This is one small file per completed event — not one file per snapshot — so the listing stays fast regardless of how many snapshots exist.

### Step 4 — Decide what to process

**Force-rebuild check (manual run only):**
If `event_id` is passed and that event already has a complete marker, the marker is deleted before building the work list. This ensures all snapshots for that event are treated as unprocessed and rerun from scratch.

**Complete events:**
Any event whose `event_id` appears in the complete marker set is skipped entirely. All its snapshots are ignored in one check — no per-snapshot comparison needed.

**Active match check (nightly run only):**
Any match whose most recent snapshot was written less than 60 minutes ago is considered still active and is skipped entirely. Only matches quiet for over 60 minutes are eligible.

**When `event_id` is set (manual run):**
The 60-minute check is skipped. That match is always included.

The remaining snapshots are sorted **oldest first** so the timeline is built in the correct chronological order.

### Step 5 — Process each snapshot (128 threads in parallel)

For each snapshot, the notebook does three things:

**a) Download the raw bronze files**

Reads up to 8 files from the snapshot folder. Old captures used different filenames, so it tries the current name first and falls back to the old name if needed:

| What it reads | Current filename | Old filename fallback |
|---|---|---|
| Live event list | `api_inplay_event_list.json` | `events_inplay_full.json` → `events_inplay.json` |
| Bet365 market odds | `api_live_market_odds.json` | `bet365_event_by_fi.json` → `bet365_event.json` |
| Bet365 game state (PG) | `api_live_market_stats.json` | _(optional)_ |
| BetsAPI match odds | `api_event_odds.json` | `event_odds_by_event_id.json` → `event_odds.json` |
| Match details | `api_event_view.json` | `event_view_by_event_id.json` |
| Odds summary | `api_event_odds_summary.json` | `event_odds_summary_by_event_id.json` |
| Capture lineage | `lineage.json` | _(optional)_ |

**b) Parse — `silver_parse_snapshot()`**

Extracts structured data from the raw API payloads:
- Which innings, over, and ball is being bowled (from the `PG` field)
- Batting team's runs, wickets, extras, run rate
- Bowling team's figures
- Required run rate and target (innings 2)
- Full market list from Bet365 decoded from the flat record stream
- Final score if the match has ended (prioritises `event_view` over `inplay` as it is more reliable at end of match)

**c) Write silver outputs — `silver_write_outputs()`**

Writes all the parsed files listed in the "What it produces" section above.
Also appends one row to the `innings_accumulator.json` for the innings tracker timeline. Dedup logic inside the accumulator prevents duplicate rows for the same over/score.

Individual per-snapshot marker files are **not** written by this notebook — completion is tracked at the event level (see Step 6).

### Step 6 — Write event-level complete markers

After all parallel processing finishes, the notebook groups results by `event_id`. For each event where **every snapshot succeeded** (zero failures), it writes:

```
silver/cricket/control/complete/event_id={id}.json
{
  "completed_at_utc": "...",
  "snapshot_count": 605
}
```

If any snapshot for an event failed, no complete marker is written for that event. The next nightly run will reprocess its snapshots and retry writing the marker.

### Step 7 — Progress and error reporting
Prints progress every 50 snapshots: count done, rate per second, estimated time remaining, failure count.
If a snapshot fails, the error is logged and processing continues with the next one — one bad snapshot does not stop the run.
At the end, prints how many complete markers were written and how many events had failures.

---

## Why event-level complete markers instead of per-snapshot markers

The old approach wrote one marker file per snapshot into `silver/cricket/control/processed_snapshots/`. Every run had to list all of those files to build a dedup set — a listing that grows forever as matches accumulate.

The new approach writes one file per completed event into `silver/cricket/control/complete/`. The listing on each run is proportional to the number of events (tens to hundreds), not the number of snapshots (tens of thousands). Once an event is complete, all its snapshots are skipped in a single set-membership check.
