# Cricket Ingestion — Module Reference

The pipeline is split across two locations depending on who calls the code:

| Location | Purpose | Deployed to |
|---|---|---|
| `src/functions/cricket_ingestion/` | Azure Function App — real-time bronze capture | `func-ramanuj` (timer triggers) |
| `infra/7.databricks/lib/` | Databricks library modules — batch bronze→silver→gold | Databricks DBFS via Terraform |
| `infra/7.databricks/notebooks/` | Databricks notebooks — thin wrappers that call the lib | Databricks workspace |

The Function App **only writes to bronze**. Everything from bronze onwards (silver, gold, hypothesis, ML) is done by Databricks notebooks via ADF pipelines.

---

## Function App — `src/functions/cricket_ingestion/`

### `function_app.py`
Azure Functions entry point. Wires four timer triggers. Contains no logic — only imports and registers.

| Timer | Schedule | What it calls |
|---|---|---|
| `discover_cricket_inplay` | every 5s | `bronze_discover_cricket_inplay()` |
| `capture_cricket_inplay_snapshot` | every 5s | `bronze_capture_cricket_inplay_snapshot()` |
| `discover_cricket_upcoming` | every hour | `bronze_discover_cricket_upcoming()` |
| `capture_cricket_prematch_odds` | every hour | `bronze_capture_cricket_prematch_odds()` |

---

### `inplay_capture.py`
Handles **live match capture** — the core real-time loop.

- `bronze_discover_cricket_inplay()` — calls BetsAPI inplay list, writes a manifest of currently live cricket matches to bronze
- `bronze_capture_cricket_inplay_snapshot()` — for each live match, calls BetsAPI full event page (odds + score), writes raw response to bronze as a timestamped snapshot

Writes to: `bronze/betsapi/inplay_snapshot/...`

---

### `prematch_capture.py`
Handles **pre-match discovery and ended-match indexing**.

- `bronze_discover_cricket_upcoming()` — calls BetsAPI upcoming list, writes manifest of future matches to bronze
- `bronze_capture_cricket_prematch_odds()` — fetches full event page for upcoming matches, writes pre-match odds snapshot to bronze
- `bronze_discover_cricket_ended()` — scans gold innings tracker files to build the ended-match index (no BetsAPI call)
- `gold_build_cricket_prematch_pages()` — builds gold pre-match summary pages from bronze snapshots

Writes to: `bronze/betsapi/prematch_snapshot/...`, `gold/cricket/prematch/...`, `gold/cricket/ended_index.json`

---

### `api_and_blob.py`
Shared utility used by both the Function App and Databricks library modules. Two responsibilities in one file:

**BetsAPI client:**
- `call_betsapi(path, params)` — authenticated HTTP call, handles retries
- `extract_results(payload)` — unwraps BetsAPI response envelope

**Azure Blob Storage helpers:**
- `upload_json / download_json / download_required_json / blob_exists`
- `get_named_container_client(name)` — container client for `bronze`, `silver`, or `gold`
- `utc_now()`, `format_unix_ts()`, `safe_float()` — small utilities
- `_is_innings_market()` — detects if a market name is an innings-total market

---

### `league_config.py`
Shared utility used by both the Function App and Databricks library modules. Manages the allowlist and blocklist of leagues and event IDs to track.

- `load_allowed_league_ids()` — set of league IDs the pipeline will capture
- `load_excluded_league_ids()` — leagues explicitly blocked
- `save_league_preferences(ids)` — persists the allowlist to gold blob
- `load_blocked_event_ids()` / `block_event_ids(ids)` — per-event block
- `collect_known_leagues()` — all leagues seen in bronze data, for admin UI

Reads/writes: `gold/cricket/league_config.json`

---

## Databricks Library — `infra/7.databricks/lib/`

These files are uploaded to Databricks DBFS by Terraform and imported by notebooks. They are **never called by the Function App**.

### `snapshot_parser.py`
Transforms raw bronze API responses into structured silver data. The most complex file in the pipeline.

- `silver_parse_bronze_to_silver()` — main entry: reads unprocessed bronze snapshots, parses each one, writes silver outputs
- `silver_parse_ended_matches()` — re-parses bronze data for recently-ended matches
- `silver_parse_snapshot(payload)` — core parser: extracts score, wickets, over, batting/bowling team, odds, ball-by-ball state
- `silver_write_outputs(silver, parsed)` — writes parsed snapshot to all silver paths (match_state, innings_snapshot, innings_accumulator, state files)

Writes to: `silver/cricket/inplay/...`

---

### `tracker_writer.py`
Builds and maintains the **gold innings tracker** — the authoritative per-match record used by the display app, ML, and hypothesis.

- `gold_write_innings_tracker_from_silver(gold, match_page)` — updates `innings_1.json`; when match ends, writes `innings_1_from_silver.json`
- `extract_innings_snapshot(rows, innings_num)` — extracts snapshot state for a given innings from tracker rows

Writes to: `gold/cricket/innings_tracker/event_id={id}/innings_1.json` and `innings_1_from_silver.json`

---

### `match_page_builder.py`
Builds **gold match page documents** — structured per-match summaries used by the display app.

- `gold_build_match_pages()` — builds/refreshes match pages for all live matches
- `gold_build_match_page(silver, gold, event_id)` — builds a single match page from latest silver snapshot
- `gold_write_index / gold_write_league_indexes` — rebuilds global and per-league indexes

Writes to: `gold/cricket/match_pages/event_id={id}/match_page.json`, `gold/cricket/match_index.json`

---

### `hypothesis.py`
Runs the two **statistical hypothesis analyses** on completed T20 matches.

- `extract_inn2_over6_favorite()` — hypothesis 1: does the odds favourite after 6 overs of the chase always win?
- `extract_timeout_wicket()` — hypothesis 2: does a wicket fall in the over that resumes after a strategic timeout (>180s pause)?
- `_load_accumulator_rows(event_id, silver)` — loads all innings rows from the silver accumulator (one file per match, no blob scanning)

Writes to: `gold/cricket/hypothesis/inn2_over6_favorite.json`, `gold/cricket/hypothesis/timeout_wicket.json`

---

### `gold_rebuild.py`
Helpers for rebuilding gold tracker files from silver data. Used by Databricks gold rebuild notebooks.

- `gold_rebuild_ended_matches(event_id=None)` — rebuilds `innings_1_from_silver.json` for all ended matches (or one specific match)
- `auto_rebuild_ended_innings()` — detects recently-ended matches that need their gold tracker rebuilt
- `_rebuild_innings_core(event_id)` — core rebuild: reads silver snapshots, calls `/v1/event/view` for authoritative final score with overs, writes gold tracker with `score_summary_events` as single source of truth

The `score_summary_events` field in the gold tracker is the **single source of truth** for final scores. Format: `"163/9(20),167/6(19.5)"` (innings-1-first, overs in brackets). All consumers — ended/view, ML predictor, hypothesis — read from this field.

Used by: `gold_build_ended_match`, `gold_backfill`, `gold_auto_rebuild` notebooks.

---

### `external_db_sync.py`
Writes innings data to an external PostgreSQL database (the cricket website's DB). Not part of the core blob pipeline.

- `write_to_cricwebsite_db(parsed)` — upserts innings rows into the external DB from a parsed silver snapshot

---

## Pipeline flow

```
BetsAPI
  │
  ├─ inplay_capture.py   ──► bronze/betsapi/inplay_snapshot/
  └─ prematch_capture.py ──► bronze/betsapi/prematch_snapshot/
       (Function App, every 5s / 1h)
  │
  ▼  ADF triggers Databricks
  │
  ├─ snapshot_parser.py  ──► silver/cricket/inplay/
  ├─ tracker_writer.py   ──► gold/cricket/innings_tracker/
  └─ match_page_builder.py ► gold/cricket/match_pages/
       (Databricks, every 30 min)
  │
  ▼  After match ends
  │
  ├─ hypothesis.py       ──► gold/cricket/hypothesis/
  ├─ gold_rebuild.py     ──► gold/cricket/innings_tracker/
  └─ ML notebooks        ──► gold/cricket/ml_features/
       (Databricks, daily / on-demand)
  │
  ▼
  cricket_display Function App  (reads gold only — never writes)
```
