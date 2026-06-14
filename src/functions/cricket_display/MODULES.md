# cricket_display — module guide

Azure Function App that serves all display pages and JSON APIs.
All data is **read-only** from blob storage; no writes except league toggle and prematch result annotations.

---

## Root-level modules

| File | Role |
|------|------|
| `function_app.py` | Azure Functions entry point — registers all HTTP routes |
| `storage.py` | Blob helpers: `download_json`, `upload_json`, `get_named_container_client`, BetsAPI client, time/type utilities. Also `lookup_ended_match(event_id)` + `parse_ss_final_scores(ss)` — authoritative final score lookup used by per-match pages |
| `bet365_parser.py` | Decodes the flat MG/MA/PA Bet365 record stream from a stored API payload into a readable market list for display (`extract_bet365_current_markets`) |
| `innings_tracker_writer.py` | Builds and writes gold innings tracker files (`innings_1.json`, `innings_1_from_silver.json`, index) from silver accumulator data |
| `league_config.py` | Loads league allowlist (`gold/cricket/config/league_preferences.json`), blocked event IDs, and collects known leagues across all indexes |

---

## views/ — one file per page group

| File | URL(s) | Data source(s) |
|------|--------|----------------|
| `common.py` | (shared) | Re-exports all common imports for view modules |
| `home.py` | `/`, `/home` | Static — no blob reads |
| `live_matches.py` | `/leagues/view`, `/leagues/{id}/matches/view` | `gold/cricket/leagues/index.json`, `gold/cricket/leagues/{id}/matches.json` |
| `ended.py` | `/ended/view` | `bronze/cricket/ended/latest/index.json` |
| `prematch.py` | `/prematch/view`, `/prematch/{id}/view`, `/prematch/leagues/view`, `/prematch/leagues/{id}/matches/view`, `/prematch/{id}/results` (POST) | `gold/cricket/prematch/latest/index.json`, `gold/cricket/prematch/latest/event_id={id}/prematch_dashboard.json`, `gold/cricket/prematch/results/event_id={id}/results.json`, `gold/cricket/prematch/leagues/index.json`, `gold/cricket/prematch/leagues/{id}/matches.json` |
| `innings_tracker.py` | `/matches/{id}/innings-tracker/view` | `gold/cricket/innings_tracker/event_id={id}/innings_1.json` + `innings_1_from_silver.json`; falls back to `bronze/cricket/ended/latest/index.json` for final score |
| `heatmap.py` | `/matches/{id}/heatmap/view` | `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json`, `silver/cricket/inplay/state/event_id={id}/state_*.json` |
| `match_analysis.py` | `/matches/{id}/detailed-analysis` | `gold/cricket/innings_tracker/event_id={id}/innings_1*.json` + `silver/cricket/inplay/.../team_scores.json` per snapshot; final score from `bronze/cricket/ended/latest/index.json` |
| `win_predictor.py` | `/ml/win-predictor` | `gold/cricket/ml_features/t20/win_predictor_summary.json` |
| `score_predictor.py` | `/ml/score-predictor` | `gold/cricket/ml_features/t20/score_predictor_summary.json` |
| `feature_matrix.py` | `/ml/feature-matrix` | `silver/cricket/inplay/state/event_id={id}/`, `gold/cricket/innings_tracker/` |
| `score_matrix.py` | `/ml/score-matrix` | `gold/cricket/innings_tracker/` (all matches) |
| `glossary.py` | `/ml/glossary` | Static — no blob reads |
| `hypothesis.py` | `/hypothesis/inn2-over6`, `/hypothesis/timeout-wicket` | `gold/cricket/hypothesis/inn2_over6_favorite.json`, `gold/cricket/hypothesis/timeout_wicket.json` |
| `mgmt.py` | `/mgmt/leagues/view`, `/mgmt/leagues/toggle` (POST), `/mgmt/rebuild-innings/{id}` | `gold/cricket/config/league_preferences.json` (read + write); rebuild reads `silver/cricket/inplay/.../team_scores.json` and writes `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json` |

---

## Data flow by layer

```
BetsAPI
  │
  ▼
[func-ramanuj-ingestion] — timer triggers
  │  writes bronze/betsapi/...
  ▼
bronze container
  ├── betsapi/inplay_snapshot/           ← raw inplay API payloads
  ├── betsapi/prematch_snapshot/         ← raw prematch API payloads
  ├── cricket/ended/latest/index.json    ← ended match list (read by ended.py)
  └── betsapi/control/upcoming_cricket/  ← upcoming match control file

[ADF + Databricks]
  │  reads bronze, writes silver + gold
  ▼
silver container
  └── cricket/inplay/
        ├── year=*/month=*/day=*/hour=*/event_id=*/snapshot_id=*/
        │     ├── match_state.json       ← per-snapshot state
        │     ├── team_scores.json       ← batting/bowling scores
        │     └── market_odds.json       ← odds per market
        ├── state/event_id=*/state_*.json ← ball-level market matrix (heatmap)
        └── control/event_id=*/innings_accumulator.json ← deduped timeline

gold container
  └── cricket/
        ├── leagues/                     ← live match index + per-league
        ├── prematch/latest/             ← prematch index + per-event dashboard
        ├── prematch/leagues/            ← prematch grouped by league
        ├── prematch/results/            ← user-annotated prediction results
        ├── ended/latest/index.json      ← ended index (written by Databricks)
        ├── innings_tracker/             ← per-match innings timeline + global index
        ├── ml_features/t20/             ← model summaries (win + score predictor)
        ├── hypothesis/                  ← pre-computed hypothesis result JSONs
        └── config/
              ├── league_preferences.json ← allowlist (read/write via mgmt.py)
              └── blocked_event_ids.json  ← manual block list
```

---

## Final score / match data — where does it come from?

| Data | Source blob | Written by |
|------|------------|------------|
| Final score (ended match) | `bronze/cricket/ended/latest/index.json` | `capture_ended.py` in ingestion (reads BetsAPI ended endpoint) |
| Live match scoreboard | `gold/cricket/matches/latest/event_id={id}/match_dashboard.json` | Databricks `silver_build_ended_match` notebook |
| Ball-by-ball innings timeline | `gold/cricket/innings_tracker/event_id={id}/innings_1_from_silver.json` | `innings_tracker_writer.py` (via admin rebuild) or Databricks |
| Prematch odds | `gold/cricket/prematch/latest/event_id={id}/prematch_dashboard.json` | Databricks `gold_build_prematch_pages` notebook |
| ML model results | `gold/cricket/ml_features/t20/*.json` | Databricks ML notebooks |
| Hypothesis results | `gold/cricket/hypothesis/*.json` | Databricks hypothesis notebooks |
