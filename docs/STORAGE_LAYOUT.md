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
| `api_event_history.json` | Raw response from `/v1/event/history`. Ball-by-ball timeline. |
| `api_event_stats_trend.json` | Raw response from `/v1/event/stats_trend`. Rolling stats trend. |
| `api_live_market_odds.json` | Raw response from `/v1/bet365/event?FI=…`. The live Bet365 market stream: match winner, over/under runs, player props. |
| `api_live_market_stats.json` | Raw response from `/v1/bet365/event?FI=…&stats=1`. Bet365 live stats including the PG/score field used for over tracking. |
| `api_live_market_lineup.json` | Raw response from `/v1/bet365/event?FI=…&lineup=1`. Bet365 lineup data. |
| `api_live_market_raw.json` | Raw response from `/v1/bet365/event?FI=…&raw=1`. Bet365 raw event feed. |
| `lineage.json` | API call log: what was called, with which IDs, how long it took, how many records came back. |
| `manifest.json` | Snapshot metadata: event ID, FI, snapshot time, paths to all other files in this folder. |

**Deduplication:** before writing, `api_live_market_stats` and `api_live_market_odds` are hashed and compared against the previous snapshot. If both match and a heartbeat was written in the last 5 minutes, the snapshot is skipped entirely. A heartbeat is force-written every 5 minutes regardless.

### Prematch snapshots

Path pattern:
```
betsapi/prematch_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/
```

| File | What it contains |
|---|---|
| `api_prematch_odds.json` | Raw response from `/v4/bet365/prematch?FI=…`. Prematch Bet365 markets. |
| `manifest.json` | Snapshot metadata. |

### Ended match index

| Path | What it contains |
|---|---|
| `cricket/ended/latest/index.json` | Index of recently ended cricket matches. Written by `discover_cricket_ended` in the Function App. Read by HTTP routes. |

### Control files

| Path | What it contains |
|---|---|
| `betsapi/control/active_inplay_fi/latest.json` | Current list of live matches being monitored. Written by `discover_cricket_inplay`, read by `capture_cricket_inplay_snapshot`. |
| `betsapi/control/upcoming_cricket/latest.json` | Current list of upcoming matches with Bet365 IDs. |
| `betsapi/control/snapshot_hash/event_id={event_id}.json` | MD5 hashes of the last written `api_live_market_stats` and `api_live_market_odds` payloads for this event. Used for dedup — skip writing if unchanged and heartbeat is fresh. |

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
| `match_state.json` | Match header: event ID, FI, teams, league, score summary, time status, API lineage. |
| `team_scores.json` | Structured team scoring rows: runs, wickets, overs, inning number. |
| `player_entries.json` | Parsed player data: batter stats (runs, balls, fours, sixes), bowler stats (overs, wickets, runs). |
| `market_odds.json` | Structured odds records: one row per market per selection with decimal odds, timestamp, provider. |
| `active_markets.json` | Currently live Bet365 markets: one row per odds selection with market group, name, team, selection, handicap, decimal odds. |
| `innings_snapshot.json` | Single innings data point for this snapshot: over, score, ball window, predicted total. |
| `lineage.json` | API call lineage copied from bronze. |

### Control files (per event, not per snapshot)

| Path | What it contains |
|---|---|
| `cricket/inplay/control/event_id={event_id}/last_known_markets.json` | Most recent non-empty `active_markets` snapshot. Fallback if Bet365 suspends prices mid-match. |
| `cricket/inplay/control/event_id={event_id}/innings_accumulator.json` | Running innings timeline for this event: all data points collected so far. Updated by the gold rebuild. |
| `cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json` | Marker written after a bronze snapshot is successfully parsed. Prevents reprocessing. |

---

## Gold container — website-ready data

Gold contains the smallest, fastest files. HTTP routes read from gold directly.

**Note: live match gold pages (match_dashboard, innings_1.json) are out of scope. Gold only contains ended match data.**

### Innings tracker (ended matches)

| Path | What it contains |
|---|---|
| `cricket/innings_tracker/event_id={event_id}/innings_1_from_silver.json` | Full innings timeline rebuilt from all silver snapshots after match ends. Gate file — match appears in ended view only once this exists. |
| `cricket/innings_tracker/event_id={event_id}/innings_1.json` | Innings accumulator written during gold rebuild. |

### Prematch data

| Path | What it contains |
|---|---|
| `cricket/prematch/latest/index.json` | Index of all upcoming matches that have prematch odds. |
| `cricket/prematch/latest/event_id={event_id}/prematch_dashboard.json` | All prematch markets for one match. |
| `cricket/prematch/history/event_id={event_id}/snapshot_id={snapshot_id}/prematch_dashboard.json` | Historical archive of every prematch snapshot. |
| `cricket/prematch/leagues/index.json` | Index of all leagues that have prematch data. |
| `cricket/prematch/leagues/{league_id}/matches.json` | All prematch matches for one league. |
| `cricket/prematch/results/event_id={event_id}/results.json` | User-saved Pass/Fail results for each prematch market selection. |

---

## Data flow summary

```
BetsAPI
  │
  ▼  Function App — capture_cricket_inplay_snapshot (every 5s, with dedup)
[bronze]  Raw API responses — deduplicated by stats+odds hash
  │
  ▼  ADF pl_build_ended_match — Activity 1 (daily 02:00 CET)
[silver]  Flat, typed records — only for matches quiet >1 hour (ended/inactive)
  │
  ▼  ADF pl_build_ended_match — Activity 2 (chained on Activity 1 success)
[gold]    innings_1_from_silver.json
  │
  ▼  ADF pl_build_ended_match — Activity 3 (chained on Activity 2 success)
[bronze]  cricket/ended/latest/index.json  →  read by /api/ended/view
```

Manual backfill:
```
ADF pl_backfill [event_id=optional]
  Activity 1: silver_backfill  →  silver files + markers
  Activity 2: gold_backfill    →  innings_1_from_silver.json (on success)
  (then trigger pl_build_ended_match to run Activity 3 and refresh ended index)
```

---

## Key identifiers

| Name | Also called | Used for |
|---|---|---|
| `event_id` | BetsAPI event ID, `id` | `/v1/event/view`, `/v2/event/odds`, `/v2/event/odds/summary` |
| `fi` | Bet365 ID, `bet365_id` | `/v1/bet365/event`, `/v4/bet365/prematch` |

The same match has both an `event_id` and an `fi`. BetsAPI endpoints use `event_id`; Bet365 endpoints use `fi`.
