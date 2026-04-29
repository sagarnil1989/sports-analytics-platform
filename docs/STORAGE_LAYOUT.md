# Azure Storage Layout

The app uses three Azure Blob Storage containers following a **bronze / silver / gold** pattern.

---

## Bronze container — raw API responses

Bronze holds the exact JSON that BetsAPI returned, nothing transformed. If something ever looks wrong in silver or gold, you can open the bronze file and see the original API data.

### Live match snapshots

Path pattern:
```
betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/
```

| File | What it contains |
|---|---|
| `api_inplay_event_list.json` | Raw response from `/v3/events/inplay`. Contains every live cricket match at the time of the snapshot: event IDs, team names, scores, Bet365 FIs. |
| `api_event_view.json` | Raw response from `/v1/event/view`. Detailed scoreboard for this one match: innings, overs, batters, bowlers. |
| `api_event_odds_summary.json` | Raw response from `/v2/event/odds/summary`. A compact view of available odds markets for this match. |
| `api_event_odds.json` | Raw response from `/v2/event/odds`. Full odds history timeline — how prices moved as the score changed. |
| `api_live_market_odds.json` | Raw response from `/v1/bet365/event?FI=…`. The live Bet365 market stream: match winner, over/under runs, player props. This is the richest source of market data. |
| `lineage.json` | API call log: what was called, with which IDs, how long it took, how many records came back. Useful for debugging and cost tracking. |
| `manifest.json` | Snapshot metadata: event ID, FI, snapshot time, paths to all other files in this folder. |

### Prematch snapshots

Path pattern:
```
betsapi/prematch_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/
```

| File | What it contains |
|---|---|
| `api_prematch_odds.json` | Raw response from `/v4/bet365/prematch?FI=…`. Prematch Bet365 markets: match winner, over/under totals, player runs, 1st over specials, innings scores. Captured before the match starts. |
| `manifest.json` | Snapshot metadata: event ID, FI, snapshot time, path to the odds file. |

### Control files

| Path | What it contains |
|---|---|
| `betsapi/control/active_inplay_fi/latest.json` | The current list of live matches being monitored. Written by `discover_cricket_inplay`, read by `capture_cricket_inplay_snapshot`. |
| `betsapi/control/upcoming_cricket/latest.json` | The current list of upcoming matches with Bet365 IDs. Written by `discover_cricket_upcoming`, read by `capture_cricket_prematch_odds`. |

### Raw discovery files (timestamped, for audit)

| Path pattern | What it contains |
|---|---|
| `betsapi/upcoming/sport_id={sport_id}/year=…/month=…/day=…/hour=…/events_upcoming_{ts}.json` | Full `/v3/events/upcoming` response at each run. |
| `betsapi/ended/sport_id={sport_id}/year=…/month=…/day=…/hour=…/events_ended_{ts}.json` | Full `/v3/events/ended` response at each run. |

---

## Silver container — parsed and structured data

Silver turns the raw API blobs into flat, typed JSON. Each file has a clear schema so downstream gold builds and analytics can read them without understanding BetsAPI internals.

### Per-snapshot files

Path pattern:
```
cricket/inplay/year={year}/month={mm}/day={dd}/hour={hh}/event_id={event_id}/snapshot_id={snapshot_id}/
```

| File | What it contains |
|---|---|
| `match_state.json` | Match header for this snapshot: event ID, FI, teams, league, score summary, time status, and API lineage. One row. |
| `team_scores.json` | Structured team scoring rows: runs, wickets, overs, inning number. One row per team per inning. |
| `player_entries.json` | Parsed player data from `/v1/event/view`: batter stats (runs, balls, fours, sixes), bowler stats (overs, wickets, runs). |
| `market_odds.json` | Structured odds records from `/v2/event/odds`: one row per market per selection with decimal odds, timestamp, and provider. |
| `active_markets.json` | Currently live Bet365 markets parsed from `/v1/bet365/event`: one row per odds selection with market group, market name, team, selection, handicap, and decimal odds. |
| `lineage.json` | API call lineage copied from the bronze lineage file: which APIs were called, with which identifiers, and what they returned. |

### Control files (per event, not per snapshot)

| Path | What it contains |
|---|---|
| `cricket/inplay/control/event_id={event_id}/last_known_markets.json` | The most recent non-empty `active_markets` snapshot for this event. Used as a fallback if Bet365 suspends prices mid-match so the page never shows zero markets. |
| `cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json` | Marker written after a bronze snapshot is successfully parsed. Prevents reprocessing the same snapshot on every silver run. |

---

## Gold container — website-ready data

Gold contains the smallest, fastest files. HTTP routes read from gold directly so they never have to scan thousands of historical files.

### Live match data

| Path | What it contains |
|---|---|
| `cricket/matches/latest/index.json` | Index of all currently live matches: one row per match with event ID, team names, score, league, snapshot time. Rebuilt every 10 seconds by the gold builder. |
| `cricket/matches/latest/event_id={event_id}/match_dashboard.json` | Full dashboard for one live match: match header, score, team scores, player stats, odds history, and active markets. Read by `/api/matches/{event_id}` and `/api/matches/{event_id}/view`. |
| `cricket/matches/latest/event_id={event_id}/lineage.json` | API lineage for the latest snapshot of this match. Read by `/api/matches/{event_id}/lineage`. |
| `cricket/matches/history/event_id={event_id}/year=…/…/snapshot_id={snapshot_id}/match_dashboard.json` | Historical archive of every match dashboard, one per snapshot. Not read by HTTP routes but kept for analytics. |
| `cricket/leagues/index.json` | Index of all live leagues: one row per league with match count. |
| `cricket/leagues/{league_id}/matches.json` | All live matches for one league. Read by `/api/leagues/{league_id}/matches`. |

### Prematch data

| Path | What it contains |
|---|---|
| `cricket/prematch/latest/index.json` | Index of all upcoming matches that have prematch odds. |
| `cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json` | All prematch markets for one match: flattened odds rows with category, market name, selection, header (Over/Under/Yes/No), handicap, and decimal odds. Read by `/api/prematch/{event_id}` and `/api/prematch/{event_id}/view`. |
| `cricket/prematch/history/event_id={event_id}/snapshot_id={snapshot_id}/prematch_dashboard.json` | Historical archive of every prematch snapshot. |
| `cricket/prematch/leagues/index.json` | Index of all leagues that have prematch data. |
| `cricket/prematch/leagues/{league_id}/matches.json` | All prematch matches for one league. |
| `cricket/prematch/results/event_id={event_id}/results.json` | User-saved Pass/Fail results for each prematch market selection. Written by `POST /api/prematch/{event_id}/results`. Each key is `{market_name}||{category_key}||{selection_name}||{handicap}||{Over or Under}` and the value is `pass`, `fail`, or `pending`. |

### Ended match data

| Path | What it contains |
|---|---|
| `cricket/ended/latest/index.json` | Index of recently ended cricket matches from `/v3/events/ended`. Used to populate the Ended Cricket Matches page. |

---

## Data flow summary

```
BetsAPI
  │
  ▼
[bronze]  Raw API responses — exactly as received
  │
  ▼  parse_cricket_bronze_to_silver (every 10 seconds)
[silver]  Flat, typed records — teams, players, markets, odds history
  │
  ▼  build_cricket_gold_match_pages (every 10 seconds)
[gold]    Small website-ready files — read directly by HTTP routes
```

For prematch:

```
BetsAPI /v4/bet365/prematch
  │
  ▼
[bronze]  api_prematch_odds.json
  │
  ▼  build_cricket_prematch_pages (every 60 seconds)
[gold]    prematch_dashboard.json + index files
```

---

## Key identifiers

| Name | Also called | Used for |
|---|---|---|
| `event_id` | BetsAPI event ID, `id` | `/v1/event/view`, `/v2/event/odds`, `/v2/event/odds/summary` |
| `fi` | Bet365 ID, `bet365_id` | `/v1/bet365/event`, `/v4/bet365/prematch` |

The same match has both an `event_id` and an `fi`. BetsAPI endpoints use `event_id`; Bet365 endpoints use `fi`.
