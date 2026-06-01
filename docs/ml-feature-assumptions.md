# ML Feature Assumptions — For Review

This document records the assumptions behind ML feature engineering in
`infra/9.ml/notebooks/ml_win_predictor.py`. Some of these may be wrong — marked
with ❓ where confidence is low.

---

## `inn1_pressure`

**Formula:**
```
inn1_pressure = inn1_total_score / (10 - inn1_total_wickets)
             = total runs scored / wickets remaining at end of innings 1
```

**Examples:**

| Score | Wickets lost | Wickets remaining | inn1_pressure | Interpretation |
|---|---|---|---|---|
| 200/2 | 2 | 8 | 200/8 = **25.0** | Dominant — big score, barely lost wickets |
| 180/5 | 5 | 5 | 180/5 = **36.0** | Solid innings, moderate wicket loss |
| 150/8 | 8 | 2 | 150/2 = **75.0** | Struggled — scraped 150, lost 8 wickets |
| 220/9 | 9 | 1 | 220/1 = **220.0** | Big score but nearly bowled out |
| 100/9 | 9 | 1 | 100/1 = **100.0** | Poor score AND nearly bowled out |

**Current interpretation:** Higher value = runs came at high wicket cost = batting team
was under pressure to post that total. Lower value = wickets cheap relative to runs =
comfortable, dominant innings.

**Possible issue flagged by user:** ❓ Logic may be wrong — needs correction.

---

## General T20 Assumptions (to be verified)

These are baked into the feature engineering. Please mark any that are wrong.

| # | Assumption | Confidence |
|---|---|---|
| 1 | Each team gets exactly 20 overs per innings | ✅ |
| 2 | Each team has 10 wickets; innings ends when 10 fall OR 20 overs complete | ✅ |
| 3 | Team batting 1st sets a target; team batting 2nd chases it | ✅ |
| 4 | Losing fewer wickets while scoring the same total = better/more comfortable innings | ✅ |
| 5 | A team all out (10 wickets) was under more pressure than a team 2 down at the end | ✅ |
| 6 | `inn1_pressure` captures how "costly" the 1st innings was in terms of wickets burned | ✅ |
| 7 | A high `inn1_pressure` 1st innings total is harder for the chasing team to replicate | ✅ |
| 8 | The Powerplay is overs 1–6, Middle overs are 7–15, Death overs are 16–20 | ✅ |
| 9 | Wide balls and no-balls do not count as legal deliveries (do not consume a wicket ball) | ✅ |
| 10 | A wicket ball scores 0 runs for the batting team off that delivery | ✅ |
| 11 | The match result (win/loss) is determined solely by whether the chasing team surpasses the target | ✅ |
| 12 | Super overs and rain-affected matches (DLS) are edge cases and excluded from training data | ✅ |

---

---

## `inn1_bat_dominance`

**Formula:**
```
max_SR − avg_SR   across all inn1 batsmen who faced ≥ 5 balls
SR = (runs / balls) * 100
```

**Source:** Silver state files — `silver/cricket/inplay/state/event_id={eid}/state_1_*.json`  
The S6 field in TE records (PI="1") holds `[Name#ID]:runs:balls` pairs. Each player's max runs across all snapshots is used as their final score (runs never decrease mid-innings).

**Examples (event 11658818 — LSG vs PBKS):**

| Player ID | Runs | Balls | SR |
|---|---|---|---|
| 237877 | 43 | 17 | 252.9 |
| 156172 | 72 | 43 | 167.4 |
| 218611 | 37 | 20 | 185.0 |
| 159972 | 26 | 21 | 123.8 |
| 211168 | 5 | 5 | 100.0 |
| 119542 | 2 | 6 | 33.3 |

max_SR = 252.9, avg_SR = 137.5, **dominance = 115.41**

**Interpretation:** High value = one player dominated (carrying the innings). Low value = balanced contribution from all batsmen.

---

## Notes

- Update this file after corrections are agreed
- Once assumptions are finalised, update `ml_win_predictor.py` feature engineering to match
