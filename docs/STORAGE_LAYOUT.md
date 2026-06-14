# Azure Storage Layout

The app uses three Azure Blob Storage containers following a **bronze / silver / gold** pattern.

---

## Bronze container — raw API responses

Bronze holds the exact JSON that BetsAPI returned, nothing transformed. If something ever looks wrong in silver or gold, you can open the bronze file and see the original API data. **Bronze is never modified by the pipeline** — only written to.

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
| `api_live_market_odds.json` | Raw response from `/v1/bet365/event?FI=…`. The live Bet365 market stream: match winner, over/under runs, player props. |
| `api_live_market_stats.json` | Raw response from `/v1/bet365/event?FI=…&stats=1`. Bet365 live stats including the PG/score field used for over tracking. |
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

### Ended match final data

| Path | What it contains |
|---|---|
| `betsapi/event_final/event_id={event_id}/event_view.json` | Final `/v1/event/view` response for a completed match. Authoritative source for final score and team names. Used by gold rebuild. |

### Ended match index (written by Databricks)

| Path | What it contains |
|---|---|
| `cricket/ended/latest/index.json` | Index of recently ended cricket matches. Written by `discover_cricket_ended` notebook. Read by HTTP routes. |

### Control files

| Path | What it contains |
|---|---|
| `betsapi/control/active_inplay_fi/latest.json` | Current list of live matches being monitored. Written by `discover_cricket_inplay`. |
| `betsapi/control/upcoming_cricket/latest.json` | Current list of upcoming matches with Bet365 IDs. |
| `betsapi/control/snapshot_hash/event_id={event_id}.json` | MD5 hashes of the last written `api_live_market_stats` and `api_live_market_odds` payloads. Used for dedup. |

---

## Silver container — parsed and structured data

Silver turns the raw API blobs into flat, typed JSON. Every piece of data for a match lives under a single `event_id={id}/` folder — no cricket/ prefix, no year/month/day partitioning.

### Per-snapshot files

Path pattern:
```
event_id={event_id}/snapshot_id={snapshot_id}/
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

### State files (per event, not per snapshot)

Path pattern:
```
event_id={event_id}/state/state_{innings}_{over}_{balls}_{snapshot_id}.json
```

One file per unique match state (innings + over + ball count). Used by heatmap and feature matrix views.

### Control files (per event, not per snapshot)

| Path | What it contains |
|---|---|
| `event_id={event_id}/last_known_markets.json` | Most recent non-empty `active_markets` snapshot. Fallback if Bet365 suspends prices mid-match. |
| `event_id={event_id}/innings_accumulator.json` | Running innings timeline for this event: all data points collected so far. Updated by the silver_to_gold rebuild. |

### Event-level complete markers

| Path | What it contains |
|---|---|
| `control/complete/event_id={event_id}.json` | Written once all bronze snapshots for a match have been successfully processed into silver. Subsequent pipeline runs skip the event entirely — O(events) listing instead of O(snapshots). To force a full reprocess: run `pl_backfill` with this `event_id` — the pipeline deletes the marker and reprocesses from scratch. |

---

## Gold container — website-ready data

Gold contains the smallest, fastest files. HTTP routes read from gold directly. Every piece of data for a match lives under `event_id={id}/` — no cricket/ prefix, no year/month/day partitioning.

### Per-event files

| Path | What it contains |
|---|---|
| `event_id={event_id}/innings_tracker.json` | Full innings timeline rebuilt from all silver snapshots after match ends. Contains both innings 1 and innings 2 data. The authoritative source for ended match analysis. |
| `event_id={event_id}/innings_accumulator.json` | (In silver, not gold — see silver control files above) |
| `event_id={event_id}/hypothesis_inn2_over6.json` | Per-match hypothesis 1 result: was the over-6 odds favourite the actual winner? |
| `event_id={event_id}/hypothesis_timeout_wicket.json` | Per-match hypothesis 2 result: did a wicket fall in the over after each detected strategic timeout? |

### Global index

| Path | What it contains |
|---|---|
| `index/innings_tracker.json` | Index of all ended matches with innings tracker data. Used by analytics and ML views. |

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
  ▼  ADF pl_build_ended_match — Activity 1: bronze_to_silver notebook (daily 02:00 CET)
[silver]  Flat, typed records in event_id={id}/snapshot_id={id}/
          + control/complete/event_id={id}.json (per-event complete marker)
  │
  ▼  ADF pl_build_ended_match — Activity 2: silver_to_gold notebook (chained)
[gold]    event_id={id}/innings_tracker.json
  │
  ▼  ADF pl_build_ended_match — Activity 3: discover_cricket_ended notebook (chained)
[bronze]  cricket/ended/latest/index.json  →  read by /api/ended/view
```

Manual backfill (one match or all quiet matches):
```
ADF pl_backfill [event_id=optional]
  Activity 1: bronze_to_silver  →  silver files + event-level complete marker
                                   (if event_id given: deletes complete marker first → full reprocess)
  Activity 2: silver_to_gold   →  innings_tracker.json (on success)
```

To test a new event manually before running all:
```
1. Run analysis/ended_match_table notebook → see all available event_ids
2. Run pl_backfill with one event_id
3. Check gold/event_id={id}/innings_tracker.json
4. When satisfied, run pl_build_ended_match or pl_backfill without event_id filter
```

---

## Key identifiers

| Name | Also called | Used for |
|---|---|---|
| `event_id` | BetsAPI event ID, `id` | `/v1/event/view`, `/v2/event/odds`, `/v2/event/odds/summary` |
| `fi` | Bet365 ID, `bet365_id` | `/v1/bet365/event`, `/v4/bet365/prematch` |

The same match has both an `event_id` and an `fi`. BetsAPI endpoints use `event_id`; Bet365 endpoints use `fi`.
