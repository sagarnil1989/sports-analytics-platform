# Issues Log

---

## Quick Reference

| # | Title | Area | Status |
|---|-------|------|--------|
| [1](#issue-1) | S Sudharsan shown as 2nd batsman instead of 1st | Detailed Analysis | ✅ Fixed |
| [2](#issue-2) | `[PLAYERFULLNAME#198836]` stored in gold s6 field | Silver / Bronze | ✅ Fixed |
| [3](#issue-3) | JC Butler appears twice in batting scorecard | Detailed Analysis | ✅ Fixed |
| [4](#issue-4) | JO Holder appears twice in batting scorecard | Detailed Analysis | ✅ Fixed |
| [5](#issue-5) | 1st innings score wrong — 161 shown instead of 155 | Detailed Analysis | ✅ Fixed |
| [6](#issue-6) | Death phase end score 151/8 instead of 155/8 | Detailed Analysis | ✅ Fixed |
| [7](#issue-7) | Bowling — Over 11 completely missing | Detailed Analysis | ✅ Fixed |
| [8](#issue-8) | `0]` shown as 1st batsman in 2nd innings | Detailed Analysis | ✅ Fixed |
| [9](#issue-9) | Phase boundary score wrong when snapshot missing | Detailed Analysis | ✅ Fixed |
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
✅ Fixed

The BetsAPI stream sends `[PLAYERFULLNAME#NNN]` placeholders at match start when player names have not yet loaded into the feed. These were stored verbatim through bronze → silver → gold because the `#` inside the placeholder collided with the S6 batsman separator, shredding the name into garbage tokens like `198836]`.

- **Root cause**: S6 format is `striker:runs:balls#non_striker:runs:balls`. `[PLAYERFULLNAME#NNN]:0:0` contains an internal `#` that split() treated as a batsman separator, producing a first "batsman" named `[PLAYERFULLNAME` and a second named `NNN]:0:0`.
- **Fix**: In `snapshot_parser.py`, S6 is normalised with `re.sub(r'\[PLAYERFULLNAME#(\d+)\]', r'PLAYERFULLNAME_\1', s6)` before splitting on `#`. This collapses the placeholder's internal `#` so only the real separator is seen. The normalised name `PLAYERFULLNAME_NNN` flows through silver into gold and is handled cleanly by the display layer's existing `_da_decode_s6` normalisation.
- **File**: `infra/7.databricks/lib/snapshot_parser.py` → S6 parsing block inside `silver_parse_snapshot()`

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

`score_summary_events` for live snapshots from `/v3/events/inplay` is home/away ordered, not innings-ordered. For this match Gujarat Titans (away) batted first, so home=RCB=inn2 was in `parts[0]` and away=Gujarat=inn1 was in `parts[1]`. The previous swap heuristic (`d_straight` vs `d_swapped`) was also failing because the ghost inn2 rows (Issue 10) had `score=155` which made `inn2_last_score=155`, eliminating the distance advantage for the correct assignment (tie always defaulted wrong).

- **Root fix**: `score_summary_events` for **completed** matches is populated from `/v1/event/view` which returns scores in innings order. Switched to `parse_ss_final_scores()` directly (same function used by the matches table Final Score column) — no heuristic needed. The second-pass filter (using the now-correct `inn1_runs=155`) also catches any ghost inn2 rows the first-pass filter missed.
- **File**: `views/match_analysis.py` → score resolution block (replaces distance heuristic)

---

## Issue 6
**Death phase end score shows 151/8 instead of 155/8**
✅ Fixed

The last captured snapshot before innings end was at 151/8 (final 4 runs had no snapshot). `build_phases` used the last captured snapshot as the Death phase end score. The `build_phases` fix (accepting `auth_runs` / `auth_wkts`) was correct, but it received the wrong `inn1_runs` value (RCB's score instead of 155) because the Issue 5 swap heuristic was failing.

- **Root fix**: Issue 5 fix (direct `parse_ss_final_scores`) now gives `inn1_runs=155` correctly. `build_phases` receives the right `auth_runs=155` and overrides the 151 Death phase end score.
- **File**: `views/match_analysis.py` → `build_phases()`, score resolution block

---

## Issue 7
**Bowling — Over 12 missing from over-by-over table and bowling summary**
✅ Fixed

Over 12 data was missing because `build_bowling` relied exclusively on `s8` (previous over stats) which only appears at the START of the following over. If no snapshot at the beginning of over 13 captured `s8`, over 12 was never recorded.

- **Root cause**: S8 is retrospective — it only appears in fielding-team rows at the start of the next over. One missed snapshot at over 13.0 loses an entire over's data.
- **Fix**: Added S7_bowl (`s7` from fielding team, PI=0) as a fallback. S7_bowl tracks the **current** over's running stats at every snapshot (`"Name#over_num#balls#runs#wickets"`). The last seen value per `over_num` gives the final tally for that over. `build_bowling` fills any gap left by S8 using this data.
- **Second root cause**: `extract_innings_snapshot` in `tracker_writer.py` never wrote `s7_bowl` to gold tracker rows — so for matches tracked live (not rebuilt), all gold rows had `s7_bowl=None` and the fallback did nothing.
- **Fix**: `extract_innings_snapshot` now extracts `s6`, `s7_bat`, `s8`, `s7_bowl` from the batting/bowling team_score rows and includes them in every gold tracker row. The duplicate extraction in `gold_rebuild.py` was removed.
- **Files**: `views/match_analysis.py`, `infra/7.databricks/lib/tracker_writer.py`, `infra/7.databricks/lib/gold_rebuild.py`
- **Note**: Existing gold data for matches tracked before this fix won't have `s7_bowl`. Trigger `gold_rebuild` for any affected match to backfill it.

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
✅ Fixed

In the 2nd innings of event 11963258, data is missing for overs 14.4–14.5. A snapshot exists at 15.1 with score=139, wickets=5, ball_window=`[0, 4, 0, 2, 0, 0]` (newest first). The old code used the last captured snapshot within each phase as the boundary score, so Middle (7-15) was ending at the over 14.3 score rather than 139.

- **Boundary inference fix**: `build_phases` finds the first snapshot of the next phase and subtracts runs from balls already bowled in the new over to infer the true end-of-phase score. Only applied when the inferred score is higher than the last captured snapshot within the phase.
- **Second root cause (why inference was still wrong)**: The boundary inference was finding the mislabeled over-19.x rows (from Issue 10, score=inn1_total, tagged innings=2) as the "first Death phase snapshot" instead of the real over-15.x snapshot. Subtracting 5 balls' runs from score=155 gave a completely wrong Middle boundary.
- **Fix**: The Issue 10 pre-filter now runs **before** `build_phases` (see Issue 5 fix), so `inn2_rows` is clean when boundary inference runs.
- **File**: `views/match_analysis.py` → `build_phases()`, pre-filter block

---

## Issue 10
**Inn 2 row at over 19.5 showing innings 1 data (score 155/8)**
✅ Fixed

At the very last ball of innings 1, the BetsAPI sets S3 (target) before PG resets to 0.0. The silver parser detects S3 present → assigns `innings=2`. These rows have over=19.x with the inn1 final score (155/8) and appeared in `inn2_rows`. This caused:
- Score resolution heuristic (Issue 5) to fail — mislabeled rows make `inn2_last_score` = inn1 total, creating a tie in the distance comparison that defaults wrong
- Phase boundary inference (Issue 9) to pick the mislabeled 19.x row as "first Death snapshot", computing a garbage Middle-Death boundary score
- Chase Analysis row at over 19.5 with RRR=42

- **Display-layer fix**: Pre-filter removes mislabeled rows from `inn2_rows` before score resolution. First pass uses `inn1_rows[-1]` score (last captured snapshot, e.g. 151); second pass uses authoritative `inn1_runs` from `parse_ss_final_scores` (e.g. 155) to catch any the first pass missed.
- **Root fix (tracker_writer)**: `extract_innings_snapshot` now skips PI=0 (fielding-team) rows when determining batting team and score. At the transition snapshot, the batting team's TE record has `S5=''` (blank) — the function now returns `None` (snapshot skipped) rather than falling through to the fielding team's stale innings-1 total. Ghost rows are never written to the gold tracker for matches tracked after this fix.
- **Files**: `views/match_analysis.py` → pre-filter block; `infra/7.databricks/lib/tracker_writer.py` → `extract_innings_snapshot()`

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
