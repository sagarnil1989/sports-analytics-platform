# Odds Movement Analysis — Feature Description

## Overview

Analyse how match-winner odds swing during a T20 match and identify matches, leagues,
and teams where the movement is greatest. The core insight is a **double-opportunity
event**: a moment where both teams' odds cross the 2.0 (evens) threshold at different
points in the same match — meaning a bettor who backed Team A while its odds were
above 2.0 and also backed Team B while its odds were above 2.0 would recover their
stake on whichever team eventually wins, with the losing bet subsidised by the
better-than-evens price obtained on the winner.

---

## Core Concept: The Double-Opportunity

A double-opportunity occurs when both of these conditions are met **within the same
match**, not necessarily at the same time:

- **At some snapshot during the match**, Team A's decimal odds ≥ 2.0 (i.e. implied
  win probability ≤ 50%).
- **At a different snapshot**, Team B's decimal odds ≥ 2.0.

If you placed equal stakes on both teams at those moments:

```
Profit = (stake × odds_on_winner) − (stake × 2)
```

This is positive whenever the odds obtained on the winning team exceed 2.0, regardless
of which team wins. The larger the odds obtained on the winner, the greater the profit.

**Example:**

| Snapshot | Team A odds | Team B odds | Scenario |
|---|---|---|---|
| Over 3 (inn1) | 1.60 | 2.40 | Back Team B at 2.40 |
| Over 9 (inn1) | 2.20 | 1.70 | Back Team A at 2.20 |
| End of match | Team A wins | | Net: 2.20 − 2.00 = +0.20 per unit stake |

---

## Analysis Goals

### 1. Match-Level: Greatest Odds Swing

For each match, compute the **maximum odds swing** — the largest change in win
probability observed across all in-play snapshots:

- **Peak odds for each team** — the highest decimal odds reached at any point.
- **Swing magnitude** — peak odds of the underdog at peak minus the minimum odds at
  any other moment.
- **Double-opportunity flag** — did both teams' odds exceed 2.0 at any point?
- **Profit potential** — if both peak-odds bets were placed, what would the net
  return have been?

Rank all matches by swing magnitude to surface the most volatile.

---

### 2. League/Tournament: Where Is Movement Highest?

Aggregate swing metrics by league/tournament to answer:

- Which leagues produce the most volatile, swing-heavy matches?
- Does league competitiveness (as measured by starting odds proximity) correlate with
  higher in-play swing?
- Are higher-profile leagues (e.g. IPL) more or less volatile than domestic T20s?

---

### 3. Team: Which Teams Are Most Associated with Swings?

Aggregate swing metrics by team (both batting-first and chasing) to identify:

- Teams that are most often involved in high-swing matches — either as the team whose
  odds collapsed and recovered, or the team that was briefly a heavy favourite and
  then a heavy underdog.
- Whether a specific team tends to produce swings as the **batting** side (e.g.
  middle-order collapse) versus the **chasing** side (e.g. early wickets in the
  chase).

---

### 4. Starting Odds Proximity: Does a Close Pre-Match Line Predict High Swing?

Hypothesis: when the pre-match odds are very close to even (both teams near 2.0),
the match is more evenly contested and therefore more susceptible to large in-play
swings. Measure:

- **Pre-match odds gap** — |Team A pre-match odds − Team B pre-match odds|.
- **In-play swing** — maximum swing observed during the match.
- **Correlation** — is a smaller pre-match gap associated with a larger in-play swing?

---

## Metrics to Compute Per Match

| Metric | Description |
|---|---|
| `peak_odds_team_a` | Maximum decimal odds reached by Team A at any snapshot |
| `peak_odds_team_b` | Maximum decimal odds reached by Team B at any snapshot |
| `min_odds_team_a` | Minimum decimal odds (i.e. heaviest favourite moment) for Team A |
| `min_odds_team_b` | Minimum decimal odds for Team B |
| `max_swing` | max(peak_odds_team_a, peak_odds_team_b) − 2.0, clipped at 0 — extra return above even-money |
| `double_opportunity` | Boolean: both teams ever exceeded 2.0 odds in this match |
| `net_profit_if_both_backed` | (peak winner odds − 2.0) × stake, if double_opportunity is true |
| `prematch_odds_gap` | Absolute difference between Team A and Team B pre-match odds |
| `swing_over` | The in-play over number at which the biggest single-snapshot odds move occurred |
| `innings_of_swing` | Was the biggest swing in innings 1 or innings 2? |
| `winner` | Actual match winner |

---

## Data Sources

All metrics are derivable from the gold `innings_tracker.json` rows already captured
per match:

- `batting_team_odds` / `bowling_team_odds` — live match-winner odds per snapshot.
- `home_team_odds` / `away_team_odds` — alternative orientation per snapshot.
- `over` / `innings` — timing of each snapshot.
- Pre-match odds: from the `bronze/betsapi/prematch_snapshot/` blobs captured by
  `capture_cricket_prematch_odds` (BetsAPI `/v4/bet365/prematch`).

No new ingestion is required — this is a pure analysis over existing gold/bronze data.

---

## Output / Deliverables

1. **Gold blob**: `gold/cricket/analysis/odds_movement_summary.json`
   — one record per match, all metrics above plus league/team identifiers.

2. **Display page**: `/api/ml/odds-movement` (or `/api/analysis/odds-movement`)
   — filterable table ranked by swing magnitude, with league and team filter dropdowns.
   - Tab 1: Match-level ranking (highest swing first).
   - Tab 2: League aggregates (avg swing, % double-opportunity matches).
   - Tab 3: Team aggregates (avg peak odds, % matches with a double-opportunity
     involving this team).
   - Tab 4: Starting-odds proximity scatter — pre-match gap vs in-play swing.

3. **Hypothesis notebook**: `infra/7.databricks/notebooks/hypothesis_odds_movement.py`
   — batch analysis run as part of `pl_ml_and_hypothesis`, writing the gold blob above.
   Would add a new activity `OddsMovementAnalysis` to the pipeline.

---

## Open Questions

- **Pre-match snapshot availability**: not all matches have a captured pre-match
  snapshot (ingestion started partway through data history). Matches without a
  pre-match snapshot can still have in-play swing computed; the starting-odds-proximity
  analysis would be limited to the subset with pre-match data.
- **Which odds to use for "peak"**: `batting_team_odds` / `bowling_team_odds` reflect
  the currently batting/fielding team, not always a stable home/away orientation.
  `home_team_odds` / `away_team_odds` are more stable for consistent team-level
  aggregation.
- **Snapshot frequency**: odds snapshots are captured every ~5 seconds during live
  play. Very short spikes (e.g. a single snapshot at 3.0 before odds corrected) should
  be smoothed or require a minimum dwell time before counting as a true "peak".
- **Abandoned / rain-affected matches**: these may show artificially large swings due
  to DLS recalculation. Worth flagging separately.
