# Cricket Ingestion — Module Reference

`src/functions/cricket_ingestion/` is deployed as Azure Function App `func-ramanuj-ingestion`.
**It only writes to bronze.** Silver and gold are owned by Databricks.

---

## Files at a Glance

| File | Group | Responsibility |
|------|-------|---------------|
| `util.py` | Foundation | Shared utilities — BetsAPI client, blob helpers, env helpers, time helpers |
| `league_config.py` | Foundation | League allowlist/blocklist management |
| `capture_inplay.py` | Capture 1 — every 5s | Captures live match snapshots → bronze |
| `capture_prematch.py` | Capture 2 — hourly | Captures upcoming match odds → bronze |
| `capture_ended.py` | Capture 3 — daily | Captures ended match master files + rebuilds ended index → bronze |
| `function_app.py` | Entry point | Wires timer triggers — no logic of its own |

---

## `function_app.py` — Timer Triggers

Five timers, all wired here, no logic of their own:

| Timer function | Schedule | Calls |
|----------------|----------|-------|
| `discover_cricket_inplay` | every 5s | `bronze_discover_cricket_inplay()` |
| `capture_cricket_inplay_snapshot` | every 5s | `bronze_capture_cricket_inplay_snapshot()` |
| `discover_cricket_upcoming` | 06:00 UTC daily | `bronze_discover_cricket_upcoming()` |
| `capture_cricket_prematch_odds` | 06:10 UTC daily | `bronze_capture_cricket_prematch_odds()` |
| `capture_ended_event_view` | 02:30 UTC daily | `bronze_capture_ended_event_view()` |

---

## `capture_inplay.py` — Live Match Bronze Capture

```
bronze_discover_cricket_inplay()
  → GET /v3/events/inplay
  → writes bronze/betsapi/inplay_filter/.../events_inplay_{ts}.json   (raw)
  → writes bronze/betsapi/control/active_inplay_fi/latest.json        (which matches are live)

bronze_capture_cricket_inplay_snapshot()
  → reads bronze/betsapi/control/active_inplay_fi/latest.json
  → for each live match:
      GET /v1/bet365/event?fi={fi}         (odds + score)
      GET /v1/event/view?event_id={id}     (event metadata)
      + 7 more API calls per match
      writes bronze/betsapi/inplay_snapshot/.../manifest.json + all api_*.json files
      STOPS HERE — no silver or gold
```

Private helpers (inplay-specific, not exported):

| Helper | What it does |
|--------|-------------|
| `_get_fi(item)` | Extracts Bet365 `fi` ID from a BetsAPI inplay item |
| `_get_event_id(item)` | Extracts `event_id` from a BetsAPI inplay item |
| `summarize_inplay_items(items)` | Filters live match list, caps at `MAX_LIVE_MATCHES` |
| `_payload_hash(data)` | MD5 of API response body — used to skip duplicate snapshots |
| `_build_snapshot_lineage(...)` | Builds lineage metadata dict embedded in every manifest |
| `_build_api_call_lineage(...)` | One lineage entry per API call |
| `_get_api_result_count(payload)` | Counts results in an API response for lineage |

---

## `capture_prematch.py` — Upcoming Match Bronze Capture

```
bronze_discover_cricket_upcoming()
  → GET /v3/events/upcoming?sport_id=3
  → writes bronze/betsapi/upcoming/.../events_upcoming_{ts}.json       (raw)
  → writes bronze/betsapi/control/upcoming_cricket/latest.json         (control)

bronze_capture_cricket_prematch_odds()
  → reads bronze/betsapi/control/upcoming_cricket/latest.json
  → for each upcoming match in allowed leagues:
      GET /v4/bet365/prematch?fi={fi}
      writes bronze/betsapi/prematch_snapshot/.../manifest.json
      writes bronze/betsapi/prematch_snapshot/.../api_prematch_odds.json
```

Internal helpers:

| Helper | What it does |
|--------|-------------|
| `summarize_event_item(item)` | Extracts key fields from a raw BetsAPI event object |
| `summarize_event_items(items)` | Filters and summarises a list of events |
| `_list_prematch_manifest_paths()` | Scans bronze for prematch manifest blobs |
| `_extract_prematch_result()` | Pulls result fields from a prematch payload |
| `parse_prematch_markets()` | Parses odds lines from a `/v4/bet365/prematch` response |
| `build_prematch_snapshot()` | Assembles a structured prematch snapshot dict |
| `gold_write_prematch_indexes()` | Writes gold prematch index files (not currently triggered) |
| `gold_build_cricket_prematch_pages()` | Builds gold prematch pages from bronze (not currently triggered) |

---

## `capture_ended.py` — Ended Match Bronze Capture

```
bronze_capture_ended_event_view()
  → scans gold/cricket/innings_tracker/ for innings_1_from_silver.json files  (get event IDs)
  → for each event with no master file yet:
      GET /v1/event/view?event_id={id}
      if time_status == "3" (confirmed ended):
          writes bronze/betsapi/event_final/event_id={id}/event_view.json     (master — never overwritten)
  → calls bronze_discover_cricket_ended()

bronze_discover_cricket_ended()    ← internal, called by bronze_capture_ended_event_view
  → reads every gold/cricket/innings_tracker/.../innings_1_from_silver.json
  → detects batting order from rows[].batting_team, swaps score if away batted first
  → writes bronze/cricket/ended/latest/index.json                             (what ended/view reads)
```

---

## `util.py` — Shared Utilities

**BetsAPI client:**

| Function | What it does |
|----------|-------------|
| `call_betsapi(path, params)` | Authenticated HTTP GET. Returns `{"request":{...}, "response":{"body":{...}}}` |
| `extract_results(payload)` | Unwraps the BetsAPI envelope → returns `payload["response"]["body"]["results"]` |

**Blob storage helpers:**

| Function | What it does |
|----------|-------------|
| `get_named_container_client(name)` | Returns container client for `"bronze"`, `"silver"`, or `"gold"` |
| `get_bronze_container_client()` | Shortcut for bronze container |
| `upload_json(container, path, data)` | Serialises dict to JSON and uploads to blob |
| `download_json(container, path)` | Downloads blob and parses JSON, returns None if missing |
| `download_required_json(container, path)` | Same but raises if missing |
| `blob_exists(container, path)` | Returns True/False |

**Environment and type helpers:**

| Function | What it does |
|----------|-------------|
| `get_env(name)` | Required env var — raises ValueError if missing |
| `get_int_env(name, default)` | Optional int env var |
| `get_bool_env(name, default)` | Optional bool env var |
| `safe_float(value)` | Converts to float, returns None on failure |
| `utc_now()` | `datetime.now(UTC)` |
| `format_unix_ts(ts)` | Unix timestamp → ISO string |
| `ts_compact(dt)` | `"20260531T143022Z"` format for filenames |

---

## `league_config.py` — League Filter

Manages which leagues are captured.

| Function | What it does |
|----------|-------------|
| `load_allowed_league_ids()` | Set of league IDs the pipeline will capture |
| `save_league_preferences(ids)` | Persists allowlist to blob |
| `load_blocked_event_ids()` | Per-event blocklist |
| `block_event_ids(ids)` | Adds event IDs to blocklist |
| `collect_known_leagues()` | All leagues seen in bronze data (for admin UI) |

Reads/writes: `gold/cricket/config/league_preferences.json`

---

## Full Function Call Flow

```
Timer: every 5s
  discover_cricket_inplay
    └─ bronze_discover_cricket_inplay()
         └─ call_betsapi("/v3/events/inplay")
         └─ upload_json → bronze/betsapi/control/active_inplay_fi/latest.json

Timer: every 5s
  capture_cricket_inplay_snapshot
    └─ bronze_capture_cricket_inplay_snapshot()
         └─ download_json ← bronze/betsapi/control/active_inplay_fi/latest.json
         └─ for each match:
              call_betsapi("/v1/bet365/event") + 8 more APIs
              upload_json → bronze/betsapi/inplay_snapshot/.../

Timer: every 1h
  discover_cricket_upcoming
    └─ bronze_discover_cricket_upcoming()
         └─ call_betsapi("/v3/events/upcoming")
         └─ upload_json → bronze/betsapi/control/upcoming_cricket/latest.json

Timer: 06:10 UTC daily
  capture_cricket_prematch_odds
    └─ bronze_capture_cricket_prematch_odds()
         └─ download_json ← bronze/betsapi/control/upcoming_cricket/latest.json
         └─ for each match:
              call_betsapi("/v4/bet365/prematch")
              upload_json → bronze/betsapi/prematch_snapshot/.../
         STOPS HERE — gold is built by ADF pipeline pl_build_prematch_pages (pending creation)

Timer: 02:30 UTC daily
  capture_ended_event_view
    └─ bronze_capture_ended_event_view()
         └─ scan gold/cricket/innings_tracker/ → get ended event IDs
         └─ for each event with no master file:
              call_betsapi("/v1/event/view")
              upload_json → bronze/betsapi/event_final/event_id={id}/event_view.json
         └─ bronze_discover_cricket_ended()
              └─ download_json ← all gold/cricket/innings_tracker/.../innings_1_from_silver.json
              └─ upload_json → bronze/cricket/ended/latest/index.json
```
