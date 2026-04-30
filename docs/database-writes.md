# CricWebsite oddsdb — What Gets Written and From Where

Written by `write_to_cricwebsite_db()` in `function_app.py`, called every silver snapshot cycle (~every 30s per live match).

---

## tbl_matches

Upserted every snapshot. Conflict key: `event_id`.

| Column | Value Written | Source Field | API Call |
|---|---|---|---|
| `event_id` | Bet365 match ID | `match.event_id` | `/v3/events/inplay` |
| `home_team_name` | e.g. "Mumbai Indians" | `match.home_team_name` | `/v3/events/inplay` |
| `away_team_name` | e.g. "Sunrisers Hyderabad" | `match.away_team_name` | `/v3/events/inplay` |
| `season_name` | League name | `match.league_name` | `/v3/events/inplay` |
| `status` | "inprogress" / "finished" | `match.time_status` (1→inprogress, 3→finished) | `/v3/events/inplay` |
| `venue` | Ground name | `match.venue` | `/v1/event/view` |
| `formate` | "T20" / "ODI" / "default" | Derived from `match.league_name` | `/v3/events/inplay` |
| `start_timestamp` | Match start UTC | `match.event_time_utc` | `/v3/events/inplay` |
| `home_score` | e.g. "243-5" | Parsed from `score_summary_events` (home part) | `/v3/events/inplay` |
| `away_score` | e.g. "249-4" | Parsed from `score_summary_events` (away part, after comma) | `/v3/events/inplay` |

---

## tbl_match_stats

Inserted once per unique `(match_id, inning_no, over)`. Conflict: `DO NOTHING` — so if the over hasn't changed since last snapshot, no new row is written and `tbl_market_odds` is also skipped for that snapshot.

| Column | Value Written | Source Field | API Call |
|---|---|---|---|
| `match_id` | FK from tbl_matches | Returned by tbl_matches upsert | — |
| `inning_no` | 1 or 2 | Comma in score_summary → inning 2, else inning 1 | `/v3/events/inplay` |
| `over` | e.g. "14.5" | **Priority 1:** `PG` field on TE record, parsed as `(N-1).(B-1)` after `#` | `/v1/bet365/event?FI=&stats=1` |
| `over` | fallback | **Priority 2:** `S3` field on TE record (e.g. "14.3" or plain "14") | `/v1/bet365/event?FI=` |
| `score` | Runs | `S5` field parsed as `runs#wickets` (left of `#`) | `/v1/bet365/event?FI=` |
| `wicket` | Wickets | `S5` field parsed as `runs#wickets` (right of `#`) | `/v1/bet365/event?FI=` |

---

## tbl_market_odds

**⚠️ Only written when a new `tbl_match_stats` row is inserted** (i.e. `stats_id` is not None). If the over hasn't changed since the last snapshot, `tbl_match_stats` hits `ON CONFLICT DO NOTHING`, `stats_id` stays None, and market odds for that snapshot are skipped entirely.

Two markets are written per eligible snapshot:

### Market 1 — Full time (match winner) — market_id = 16

| Column | Value Written | Source Field | API Call |
|---|---|---|---|
| `match_stats_id` | FK from tbl_match_stats | — | — |
| `market_id` | 16 | Looked up from `tbl_markets` by name "Full time" | — |
| `option` | e.g. "Match Winner 2-Way" | `market_group_name` | `/v1/bet365/event?FI=` |
| `choice` | "1" (home) or "2" (away) | Derived: selection_name contains home team name → "1" | `/v1/bet365/event?FI=` |
| `odd` | e.g. 1.285 | `odds_decimal` | `/v1/bet365/event?FI=` |
| `is_suspended` | true / false | `suspended` flag on market row | `/v1/bet365/event?FI=` |

Only non-suspended rows are written for this market.

### Market 2 — Current innings runs (innings total) — market_id = 2

| Column | Value Written | Source Field | API Call |
|---|---|---|---|
| `match_stats_id` | FK from tbl_match_stats | — | — |
| `market_id` | 2 | Looked up from `tbl_markets` by name "Current innings runs" | — |
| `option` | e.g. "Mumbai Indians 20 Overs Runs" | `market_group_name` | `/v1/bet365/event?FI=` |
| `choice` | "over" or "under" | Parsed from `display_selection_name` (e.g. "Over 208.5" → "over") | `/v1/bet365/event?FI=` |
| `odd` | e.g. 1.909 | `odds_decimal` | `/v1/bet365/event?FI=` |
| `detail` | e.g. "208.5" | `line` / `handicap` field (the innings total line) | `/v1/bet365/event?FI=` |
| `is_suspended` | true / false | `suspended` flag | `/v1/bet365/event?FI=` |

Suspended rows ARE written here (batting team's market is routinely suspended mid-over).

---

## Known Gap

`tbl_market_odds` rows are skipped for any snapshot where the over number hasn't changed (same over, different ball). This means odds fluctuations within the same over are not captured in the DB — only the first snapshot of each over is recorded.

To fix: change `tbl_match_stats` conflict from `DO NOTHING` to `DO UPDATE` (update score/wicket), then always write market odds using the existing `match_stats_id` rather than only writing when a new row is created.
