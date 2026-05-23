# Function App — Timer Triggers

Silver and gold processing run in ADF + Databricks, not here. The Function App handles bronze capture and HTTP routes only.

---

## Bronze Timers

| Function | Schedule | Calls | Container | Files Written |
|---|---|---|---|---|
| `discover_cricket_inplay` | Every 5s | `bronze_discover_cricket_inplay()` | bronze | `betsapi/control/active_inplay_fi/latest.json`<br>`betsapi/inplay/sport_id=3/…/events_inplay_{ts}.json` |
| `capture_cricket_inplay_snapshot` | Every 5s | `bronze_capture_cricket_inplay_snapshot()` | bronze | `betsapi/inplay_snapshot/sport_id=3/event_id={eid}/fi={fi}/snapshot_id={ts}/`<br>→ `api_inplay_event_list.json`<br>→ `api_event_view.json`<br>→ `api_event_odds_summary.json`<br>→ `api_event_odds.json`<br>→ `api_event_history.json`<br>→ `api_event_stats_trend.json`<br>→ `api_live_market_odds.json`<br>→ `api_live_market_stats.json`<br>→ `api_live_market_lineup.json`<br>→ `api_live_market_raw.json`<br>→ `lineage.json`<br>→ `manifest.json`<br><br>**Skipped (dedup):** if `api_live_market_stats` and `api_live_market_odds` hashes match the previous snapshot AND last write was < 5 min ago — writes `betsapi/control/snapshot_hash/event_id={eid}.json` only |
| `discover_cricket_upcoming` | Every 1 hour (on the hour) | `bronze_discover_cricket_upcoming()` | bronze | `betsapi/control/upcoming_cricket/latest.json`<br>`betsapi/upcoming/sport_id=3/…/events_upcoming_{ts}.json` |
| `capture_cricket_prematch_odds` | Every 1 hour (+10 min) | `bronze_capture_cricket_prematch_odds()` | bronze | `betsapi/prematch_snapshot/sport_id=3/event_id={eid}/fi={fi}/snapshot_id={ts}/`<br>→ `api_prematch_odds.json`<br>→ `manifest.json` |
| `discover_cricket_ended` | Every 1 hour (+20 min) | `bronze_discover_cricket_ended()` | **bronze** | `cricket/ended/latest/index.json` |

---

## ADF + Databricks Pipelines

| Pipeline | Schedule | Activities | Files Written |
|---|---|---|---|
| `pl_build_ended_match` | Daily 02:00 CET | **Activity 1:** `silver_build_ended_match` notebook — parse bronze → silver for matches quiet >1 hour<br>**Activity 2:** `gold_build_ended_match` notebook — rebuild gold from silver (only if Activity 1 succeeds) | Silver: `silver/cricket/inplay/…/*.json` + processed_snapshot markers<br>Gold: `gold/cricket/innings_tracker/event_id={eid}/innings_1_from_silver.json` |
| `pl_backfill` | Manual only | **Activity 1:** `silver_backfill` notebook — silver backfill (optional event_id, loops 4hr if empty)<br>**Activity 2:** `gold_backfill` notebook — gold rebuild for same scope (only if Activity 1 succeeds) | Same as above |
