# Issues Log

---

## Quick Reference

| # | Title | Area | Status |
|---|-------|------|--------|
| [1](#issue-1) | S Sudharsan shown as 2nd batsman instead of 1st | Detailed Analysis | ✅ Fixed |
| [2](#issue-2) | `[PLAYERFULLNAME#198836]` stored in gold s6 field | Silver / Bronze | 🔴 Open |
| [3](#issue-3) | JC Butler appears twice in batting scorecard | Detailed Analysis | ✅ Fixed |
| [4](#issue-4) | JO Holder appears twice in batting scorecard | Detailed Analysis | ✅ Fixed |
| [5](#issue-5) | 1st innings score wrong — 161 shown instead of 155 | Detailed Analysis | ✅ Fixed |
| [6](#issue-6) | Death phase end score 151/8 instead of 155/8 | Detailed Analysis | ✅ Fixed |
| [7](#issue-7) | Bowling — Over 11 completely missing | Detailed Analysis | 🔴 Open |
| [8](#issue-8) | `0]` shown as 1st batsman in 2nd innings | Detailed Analysis | ✅ Fixed |
| [9](#issue-9) | Phase boundary score wrong when snapshot missing | Detailed Analysis | 🔴 Open |
| [10](#issue-10) | Inn 2 row at over 19.5 showing innings 1 data | Detailed Analysis | ✅ Fixed |
| [11](#issue-11) | Win% in Chase Analysis — formula explained | Detailed Analysis | 📖 Doc |

---

## Issue 1
**S Sudharsan shown as 2nd batsman instead of 1st**
✅ Fixed

At over 0.0, s6 is `[PLAYERFULLNAME#198836]:0:0#[PLAYERFULLNAME#176362]:0:0`. The placeholder `[PLAYERFULLNAME#198836]` contains `#`, which is the s6 separator. `split("#")` shredded the string into `["[PLAYERFULLNAME", "198836]:0:0", ...]`, giving the first batsman a garbled name `198836]`. This broken ID failed the striker resolution mapping, so S Sudharsan (the real striker from `s7_bat`) was never linked to position 1. S Gill was picked up later and appeared first due to set ordering.

- **Fix**: `_da_decode_s6` now normalises `[PLAYERFULLNAME#NNN]` → `NNN` using regex before splitting on `#`. Batting tracker now preserves s6 insertion order (index 0 = striker) instead of using unordered set subtraction.
- **File**: `views/match_analysis.py` → `_da_decode_s6()`, `build_batting()`

---

## Issue 2
**`[PLAYERFULLNAME#198836]` stored in gold s6 field**
🔴 Open — bronze / silver data issue

The BetsAPI stream sends `[PLAYERFULLNAME#NNN]` placeholders at match start when player names have not yet loaded into the feed. These are stored verbatim through bronze → silver → gold. Later snapshots in the same match have real names.

- **Impact**: First few snapshots of any innings have unresolvable player IDs in s6. Issue 1 was a symptom of this.
- **To investigate**: Check bronze TE records at over 0.0–0.2 for any match. Confirm at what over the API starts sending real names.
- **Proposed fix**: In `bronze_to_silver.py`, skip or delay s6 embedding for snapshots where any batsman name still matches `[PLAYERFULLNAME#...]`. Alternatively, post-process in `gold_rebuild.py` to backfill names from later snapshots.
- **Files**: `bronze_to_silver.py`, `infra/7.databricks/lib/gold_rebuild.py`

---

## Issue 3
**JC Butler appears twice in batting scorecard**
✅ Fixed

Same root cause as Issue 1. The garbled `198836]` ID was added to the batting tracker's `seen` dict. When the real name "JC Butler" appeared in a later snapshot it was treated as a new player — creating a second entry. The early entry showed 5 runs / 5 balls; the correct entry showed 19 runs / 23 balls.

- **Fix**: `_da_decode_s6` fix (Issue 1) prevents the garbled ID from ever entering `seen`. A dedup step was also added to `build_batting` — if the same name appears twice, keep the earliest entry_over and the highest-balls stats.
- **File**: `views/match_analysis.py` → `build_batting()`

---

## Issue 4
**JO Holder appears twice in batting scorecard**
✅ Fixed

Same root cause and fix as Issue 3.

- **File**: `views/match_analysis.py` → `build_batting()`

---

## Issue 5
**1st innings score shown as 161 instead of 155**
✅ Fixed

`score_summary_events` stores scores in home/away order (raw BetsAPI `ss` field), not innings order. The away team batted first in this match, so `parts[0]` (home = 161) was inn2, and `parts[1]` (away = 155) was inn1. The code was blindly using `parts[0]` as innings 1, reversing the scores. This also caused the wrong target (162 instead of 156) in chase analysis, which cascaded into RRR showing 42 instead of 6.

- **Fix**: After parsing `score_summary_events`, both scores are compared against the last captured snapshot score of each innings. The assignment that minimises total distance is used.
- **Permanent fix needed**: `gold_rebuild.py` should write a `batting_first_team_name` field so the display can determine innings order definitively instead of using a heuristic.
- **File**: `views/match_analysis.py` → score resolution block

---

## Issue 6
**Death phase end score shows 151/8 instead of 155/8**
✅ Fixed

The last captured snapshot before innings end was at 151/8. The final 4 runs (last few balls) had no snapshot. `build_phases` was using the last captured snapshot as the Death phase end score.

- **Fix**: `build_phases` now accepts `auth_runs` / `auth_wkts` (the authoritative final score from `score_summary_events`). If the Death phase `end_score` is lower than `auth_runs`, it is overridden with the authoritative value.
- **File**: `views/match_analysis.py` → `build_phases()`

---

## Issue 7
**Bowling — Over 11 completely missing from over-by-over table and bowling summary**
🔴 Open — needs bronze investigation

Over 11 (11.0–11.5) exists in the gold tracker rows but does not appear in the bowling section. Each over's bowling data comes from the `s8` field (previous over: bowler / runs / wickets) embedded in snapshots from the NEXT over. If no snapshot at the start of over 12 has `s8` populated, over 11 is never recorded.

- **To investigate**: In bronze for event 11963258, find the TE records at over `12.0`–`12.2` and check whether `S8` is present. If absent, the data was never captured in the feed at that moment.
- **Files**: Bronze snapshots for event 11963258, `infra/7.databricks/lib/gold_rebuild.py` (S8 embedding step)

---

## Issue 8
**`0]` shown as 1st batsman in 2nd innings**
✅ Fixed

At over 0.0 of innings 2, no balls have been bowled yet. The s6 field at this point often contains `[PLAYERFULLNAME#NNN]` placeholders (see Issue 2). After the Issue 1 fix, these parse to numeric IDs like `0]` or similar fragments, which were being added to the batting tracker as real players.

- **Fix**: `build_batting` now skips s6 processing entirely for rows where `over == "0.0"`.
- **File**: `views/match_analysis.py` → `build_batting()`

---

## Issue 9
**Phase boundary score wrong when exact boundary snapshot is missing (2nd innings)**
🔴 Open — deferred

In the 2nd innings of event 11963258, data is missing for overs 14.4–14.5. A snapshot exists at 15.1 with score=139, wickets=5, ball_window=`[0, 4, 0, 2, 0, 0]` (newest first). Since ball 15.1 scored 0 runs, end-of-over-14 score = 139 − 0 = 139. The phase code does not do this inference — it uses the last captured snapshot within each phase as the boundary score, so Middle (7-15) ends at over 14.3 score rather than 139.

- **Proposed fix**: When the exact `X.0` boundary snapshot is missing, find the first snapshot of over X+1 and subtract `ball_window[0]` (the run scored on that ball) to infer the end-of-over score.
- **File**: `views/match_analysis.py` → `build_phases()`

---

## Issue 10
**Inn 2 row at over 19.5 showing innings 1 data (score 155/8)**
✅ Fixed

At the very last ball of innings 1, the BetsAPI sets S3 (target) before PG resets to 0.0. The silver parser detects S3 present → assigns `innings=2`. These rows have over=19.x with the inn1 final score (155/8) and appeared in `inn2_rows`. This caused:
- Death phase 2nd innings start showing `155/8` instead of the true inn2 start score
- Chase Analysis row at over 19.5 with RRR=42 (also worsened by Issue 5's wrong target)

- **Fix applied**: `match_analysis.py` filters `inn2_rows` to exclude any row where `score == inn1_runs AND wickets == inn1_wkts` after the authoritative inn1 score is determined.
- **Root fix still needed**: `bronze_to_silver.py` innings detection should handle this edge case so mislabeled rows are never written to the gold tracker.
- **File**: `views/match_analysis.py` → inn2_rows filter block

---

## Issue 11
**Win% in Chase Analysis — what it means**
📖 Documentation only

Win% is the implied probability derived from Bet365 decimal odds for the batting (chasing) team at each snapshot during the 2nd innings.

```
Win% = (1 / batting_team_odds) × 100
```

| Odds | Win% | Meaning |
|------|------|---------|
| 1.25 | 80% | Heavy favourite |
| 2.00 | 50% | Evens |
| 4.00 | 25% | Underdog |

Odds come from the `batting_team_odds` field in each gold tracker row, captured live from `/v1/bet365/event`. Rows where the market was suspended or not captured show `—`.
