# Code Flow — Sports Analytics Platform

This document explains what each part of the codebase does, in plain English, and how data moves from BetsAPI all the way to the browser.

---

## The Three Folders and What They Own

| Folder | Deployed As | Job |
|--------|------------|-----|
| `src/functions/cricket_ingestion/` | Azure Function App `func-ramanuj-ingestion` | **Captures** raw data from BetsAPI on a schedule. Writes bronze only. |
| `src/functions/cricket_display/` | Azure Function App `func-ramanuj-display` | **Serves** HTML pages and JSON APIs to the browser. Reads from blob storage. |
| `infra/7.databricks/` | Databricks workspace on Azure | **Sole owner of silver and gold** — transforms raw bronze into clean, queryable data. |

Data only flows one way: **ingestion writes bronze → Databricks transforms to silver/gold → display reads gold**. The display app never calls BetsAPI. Databricks never serves web pages. Ingestion never writes silver or gold.

---

## Storage Layers (Bronze → Silver → Gold)

All three layers live in Azure Blob Storage (`stramanujdlweu`), each in its own container.

### Bronze — Raw API Dumps
- **Written by**: `cricket_ingestion` only
- Path pattern: `betsapi/inplay_snapshot/sport_id=1/event_id={id}/fi={fi}/snapshot_id={sid}/`
- Also: `betsapi/event_final/event_id={id}/event_view.json` — master file for final scores
- Contains the exact JSON response from every BetsAPI call, untouched

Think of bronze as a recording of every API call ever made. Nothing is ever deleted.

### Silver — Parsed Match State
- **Written by**: Databricks only — two pipelines:
  - `pl_backfill` → `silver_backfill.py` — manual, single event, processes unprocessed snapshots
  - `pl_silver_build_ended_match` → `silver_build_ended_match.py` — daily batch, all quiet/ended matches (no new snapshot in 60 min)
- Path pattern: `cricket/inplay/control/event_id={id}/innings_accumulator.json`
- Contains: structured match state — score, over, ball window, market odds — parsed from raw bronze JSON

Silver is bronze made readable. One silver file per match.

### Gold — Final Tracker Files
- **Written by**: Databricks only — two pipelines:
  - `pl_backfill` → `gold_backfill.py` — manual, single event, runs after silver backfill
  - `pl_build_ended_match` → `gold_build_ended_match.py` — nightly batch, all ended matches
- Path pattern: `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json`
- Contains:
  - **Ball-by-ball rows** (over, score, batting_team, odds, ball_window) — sourced from **silver**
  - **Final score with overs, home/away names, league, stadium** — sourced from **bronze master** (`betsapi/event_final/event_id={id}/event_view.json`), which was populated by `capture_ended_event_view()` calling BetsAPI `/v1/event/view`

Gold is what the display app reads. It is the single source of truth for everything shown in the browser.

---

## Flow 1 — Bronze Capture (During a Match)

**Triggered by**: two timer triggers in `cricket_ingestion/function_app.py`, both fire every 5 seconds.

```
Every 5 seconds:
  discover_cricket_inplay()         → calls BetsAPI /v3/events/inplay
                                    → writes bronze/betsapi/control/active_inplay_fi/latest.json
                                    → lists which matches are currently live

  capture_cricket_inplay_snapshot() → for each live match:
                                       calls BetsAPI /v1/bet365/event (odds)
                                       calls BetsAPI /v1/event/view   (scores)
                                       writes all raw responses to bronze/betsapi/inplay_snapshot/...
                                       STOPS HERE — no silver or gold written
```

**Key file**: `src/functions/cricket_ingestion/inplay_capture.py`

All parsing and transformation happens later in Databricks, not here.

---

## Flow 2 — Silver Backfill (Batch, After Match Ends)

**Triggered by**: ADF pipeline `pl_backfill` → Databricks notebook `silver_backfill.py`

```
silver_backfill.py:
  1. Scans bronze for all snapshot manifest files for the event
  2. Checks silver for which snapshots are already processed (skips them)
  3. For each unprocessed snapshot (in parallel, 128 threads):
       Downloads raw bronze files (odds JSON, event view JSON, stats JSON)
       Calls silver_parse_snapshot()  → structured match state
       Calls silver_write_outputs()   → writes to silver/
       Writes a "processed" marker so it won't be reprocessed
```

**Key files**:
- `infra/7.databricks/notebooks/silver_backfill.py` — orchestration
- `infra/7.databricks/lib/snapshot_parser.py` — parses raw bronze into silver structure
- `infra/7.databricks/lib/tracker_writer.py` → `extract_innings_snapshot()` — builds one data point per snapshot

After this step, silver has clean, complete, correctly-parsed data for every snapshot taken during the match.

---

## Flow 3 — Gold Backfill (Batch, After Silver Is Ready)

**Triggered by**: ADF pipeline `pl_backfill` → Databricks notebook `gold_backfill.py` (runs after silver backfill succeeds)

```
gold_backfill.py:
  Calls gold_rebuild_ended_matches(event_id=...)

gold_rebuild.py → _rebuild_innings_core(event_id):
  1. Reads all silver snapshots for the event
  2. For each snapshot, calls extract_innings_snapshot() from tracker_writer.py
       → gets: over, score, wickets, batting_team, bowling_team, ball_window, innings, target, odds
  3. Deduplicates rows (same over + score = same state, keep latest)
  4. Reads bronze master file betsapi/event_final/event_id={id}/event_view.json
       → gets: authoritative final score (with overs), home/away team names, league, stadium
  5. Overrides tracker with master data
  6. Writes gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json
```

**Key file**: `infra/7.databricks/lib/gold_rebuild.py`

After this step, the gold tracker has:
- All innings-1 rows with correct batting_team
- All innings-2 rows with correct batting_team and target
- Authoritative final score with overs (e.g. `"155/8(20),161/5(18)"`)
- Correct home/away team names and stadium from BetsAPI event/view

---

## Flow 4 — Ended Index Refresh

**Triggered by**: `cricket_ingestion/function_app.py` timer at 02:30 UTC daily → `capture_ended_event_view()`

```
capture_ended_event_view():
  1. Scans gold/ for all innings_1_from_silver.json files → gets list of ended event IDs
  2. For any event ID not yet in bronze/betsapi/event_final/:
       Calls BetsAPI /v1/event/view
       Writes raw response to bronze/betsapi/event_final/event_id={id}/event_view.json
  3. Calls bronze_discover_cricket_ended()

bronze_discover_cricket_ended():
  1. Reads every gold tracker (innings_1_from_silver.json)
  2. Detects which team batted first (from rows[].batting_team)
  3. Swaps score to batting-first order if away team batted first
  4. Writes bronze/cricket/ended/latest/index.json  ← this is what ended/view reads
```

**Key file**: `src/functions/cricket_ingestion/prematch_capture.py`

---

## Flow 5 — Browser Request (Display)

**Triggered by**: user opens a URL in the browser.

```
Browser → func-ramanuj-display → views/*.py → reads gold or bronze blob → renders HTML
```

### ended/view
```
GET /api/ended/view
  → views/ended.py
  → reads bronze/cricket/ended/latest/index.json
  → renders HTML table (one row per match)
```

### innings-tracker/view (per match)
```
GET /api/matches/{event_id}/innings-tracker/view
  → views/innings_tracker.py → view_silver_innings_tracker_html()
  → reads gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json
  → detects batting order from rows[].batting_team
  → renders scoreboard + ball-by-ball table for both innings
```

### ML win-predictor
```
GET /api/ml/win-predictor
  → views/ml.py
  → reads gold tracker for each event
  → renders prediction table
```

### hypothesis/inn2-over6
```
GET /api/hypothesis/inn2-over6
  → views/hypothesis.py
  → reads gold tracker for each event
  → renders analysis table
```

---

## Active Routes on func-ramanuj-display

| Route | Purpose |
|-------|---------|
| `/api/home` | Home page |
| `/api/ended/view` | All ended matches with final scores |
| `/api/matches/{id}/innings-tracker/view` | Ball-by-ball tracker for one ended match |
| `/api/prematch/view` | Upcoming matches |
| `/api/leagues/view` | Matches by league |
| `/api/ml/win-predictor` | ML model results |
| `/api/ml/feature-matrix` | ML feature data |
| `/api/ml/score-predictor` | Score prediction model |
| `/api/ml/score-matrix` | Score feature data |
| `/api/ml/glossary` | Terminology |
| `/api/hypothesis/inn2-over6` | Favourite wins hypothesis |
| `/api/hypothesis/timeout-wicket` | Wicket after timeout hypothesis |
| `/api/mgmt/leagues/view` | League filter admin |

**Removed routes** (2026-06-11): all `/api/matches/view`, `/api/matches/{id}/view`, `/api/matches/{id}/heatmap`, `/api/matches/{id}/detailed-analysis`, `/api/matches/{id}/markets/*`, `/api/innings-tracker` — live match tracking removed.

---

## Which File to Edit for Which Bug

| Symptom | Where to look |
|---------|--------------|
| Wrong data on `ended/view` | `prematch_capture.py → bronze_discover_cricket_ended()` |
| Wrong final score or overs | `gold_rebuild.py → _rebuild_innings_core()` |
| Wrong batting_team on innings-2 rows | `tracker_writer.py → extract_innings_snapshot()` AND `cricket_display/innings_tracker.py` (must fix both) |
| Wrong display on innings-tracker page | `views/innings_tracker.py → view_silver_innings_tracker_html()` |
| Wrong data on ML page | `views/ml.py` + gold tracker it reads |
| Wrong data on hypothesis page | `views/hypothesis.py` + gold tracker it reads |
| Home page sections | `views/home.py` |
| Bronze capture stops working | `inplay_capture.py` |

---

## ADF Pipelines Quick Reference

| Pipeline | What it does | Safe to run with single event_id? |
|----------|-------------|----------------------------------|
| `pl_backfill` | silver_backfill → gold_backfill for one event | Yes — designed for this |
| `pl_silver_build_ended_match` | silver rebuild for all quiet/ended matches (daily) | No — runs for ALL events |
| `pl_build_ended_match` | gold rebuild for all ended matches (nightly) | No — runs for ALL events |

**`trigger_build_ended_match` is currently STOPPED** — do not re-enable until all fixes are validated and full gold rebuild has run cleanly.
