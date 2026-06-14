# Issues Log

---

## Quick Reference

| # | Title | Area | Status |
|---|-------|------|--------|
| [1](#issue-1) | Gold rebuild not run for all events | Pipeline | 🔴 Open |
| [2](#issue-2) | Score ordering broken when tracker has no rows | Pipeline | 🔴 Open |
| [3](#issue-3) | No `batting_first_team_name` field in gold tracker | Pipeline | 🔴 Open |
| [4](#issue-4) | Phase breakdown ball stats wrong in explorer notebook | Notebook | 🔴 Open |
| [5](#issue-5) | ADF trigger `trigger_build_ended_match` is stopped | Pipeline | 🟡 Hold |
| [6](#issue-6) | Diagnostic logs filtered by App Insights | Observability | 🟡 Partial |
| [7](#issue-7) | Home page showed "Live Matches" section | Display | ✅ Fixed |
| [8](#issue-8) | Home page showed "Innings Tracker Analytics" section | Display | ✅ Fixed |
| [9](#issue-9) | ADF pipeline `pl_build_prematch_pages` not created | Pipeline | 🔴 Open |
| [10](#issue-10) | Bowling: over 11 missing from detailed-analysis | Display | 🔴 Open |
| [11](#issue-11) | Phase boundary score wrong when snapshot missing | Display | 🔴 Open |
| [12](#issue-12) | Last-ball-of-inn1 rows mislabeled as innings 2 | Display | ✅ Fixed |
| [13](#issue-13) | `score_summary_events` home-first not innings-first | Display | ✅ Fixed |
| [14](#issue-14) | Win% in Chase Analysis — formula explained | Display | 📖 Doc |

---

## Pipeline Issues

### Issue 1
**Gold rebuild not run for all events (only 1 of 80 done)**
🔴 Open

Gold tracker rebuild with authoritative scores was run only for event `11963258` as a test. The remaining ~79 ended events still have old tracker data — scores may be wrong, overs missing, stadium/league/home/away may come from silver instead of the bronze master.

- **Impact**: `ended/view`, hypothesis pages, ML pages show stale data for ~79 events
- **Fix**: Run `gold_rebuild_ended_matches()` with no `event_id` filter. Validate event 11963258 first, then run all.
- **Blocker**: Issue 5 (ADF trigger is stopped)

---

### Issue 2
**Score ordering broken when tracker has no rows**
🔴 Open

`bronze_discover_cricket_ended()` determines batting order by scanning `tracker["rows"]` for the first `innings == 1` row and reading `batting_team`. If no rows exist (silver never captured live data), `inn1_bat` stays `None` and the home team score is always shown first regardless of who batted first.

- **Impact**: Matches with sparse silver rows show wrong score order on `ended/view`
- **Fix**: Add a fallback — infer batting order from a dedicated `batting_first_team` field (see Issue 3)
- **Files**: `prematch_capture.py` → `bronze_discover_cricket_ended()`, `gold_rebuild.py`

---

### Issue 3
**No `batting_first_team_name` field in gold tracker**
🔴 Open

The gold tracker has no explicit field for which team batted first. Batting order is inferred at display time by scanning rows — fragile and impossible when rows are absent (Issue 2). `gold_rebuild.py` already has the BetsAPI `ss` field (home-first) and home/away team names, so this could be written directly.

- **Impact**: Blocks the fallback fix for Issue 2. Makes Issue 13 display fix a heuristic rather than deterministic.
- **Fix**: In `gold_rebuild.py`, compare first-innings row scores against `ss` field and write `"batting_first_team_name"` into the tracker JSON
- **Files**: `infra/7.databricks/lib/gold_rebuild.py`

---

### Issue 5
**ADF trigger `trigger_build_ended_match` is stopped**
🟡 On hold — intentional

Trigger was manually stopped on 2026-06-10 to prevent bad gold rebuilds while code issues are being fixed.

- **Action**: Re-enable only after Issues 1–3 are validated and the full gold rebuild completes cleanly
- **Command**:
  ```bash
  az datafactory trigger start \
    --factory-name adf-ramanuj \
    --resource-group rg-ramanuj \
    --name trigger_build_ended_match
  ```

---

### Issue 9
**ADF pipeline `pl_build_prematch_pages` not yet created**
🔴 Open

Prematch capture runs daily (06:00–06:10 UTC) and writes to bronze only. Nothing updates the gold prematch index (`gold/cricket/prematch/latest/index.json`) that `api/prematch/view` reads.

- **Impact**: `/api/prematch/view` shows stale data
- **Fix**: Create Databricks notebook `prematch_gold_build.py` and ADF pipeline `pl_build_prematch_pages` triggered at 07:00 UTC. Reference logic: `capture_prematch.py` → `gold_build_cricket_prematch_pages()` and `gold_write_prematch_indexes()`

---

## Observability

### Issue 6
**Diagnostic logs filtered by App Insights**
🟡 Partially fixed

Per-event API response logs were upgraded to `logging.warning` so App Insights captures them. All other diagnostic logs still use `logging.info` and may be filtered by default sampling.

- **Impact**: Debugging pipeline failures requires Databricks logs instead of App Insights
- **Fix**: Upgrade key diagnostic log lines in `prematch_capture.py` to `logging.warning`

---

## Display / Detailed Analysis

### Issue 4
**Phase breakdown ball stats wrong (Section 7 of match_data_explorer)**
🔴 Open

4s, 6s, and dot balls in Section 7 of `match_data_explorer` are wrong. The `ball_window` in the `PG` field is a rolling 6-ball window that spans over boundaries — it cannot be used directly to count events per phase.

- **Fix**: Reconstruct per-ball events from consecutive snapshot diffs rather than reading the window at a single snapshot
- **Files**: `match_data_explorer` notebook

---

### Issue 7
**Home page showed "Live Matches" section**
✅ Fixed — 2026-06-11

Removed along with all `/api/matches/...` live routes as part of the decision to drop live match tracking from the display app.

- **Files changed**: `views/home.py`, `function_app.py`, `views/__init__.py`

---

### Issue 8
**Home page showed "Innings Tracker Analytics" section**
✅ Fixed — 2026-06-11

Removed from home page. The per-match tracker (`/api/matches/{event_id}/innings-tracker/view`) is kept — it reads gold and is still useful for ended matches.

- **Files changed**: `views/home.py`, `function_app.py`, `views/__init__.py`

---

### Issue 10
**Bowling: over 11 missing from detailed-analysis (event 11963258)**
🔴 Open — needs bronze investigation

Over 11 (11.0–11.5) exists in the gold tracker rows but is absent from the Over by Over table and Bowling Summary. Each over's bowling data comes from the `s8` field (previous over) embedded in the NEXT over's snapshots. If no over-12 snapshot has `s8` populated, over 11 goes unrecorded.

- **To investigate**: In bronze for event 11963258, find TE records at over `12.0`–`12.2` and check whether the `S8` field is present
- **Files**: `infra/7.databricks/lib/gold_rebuild.py` (S8 embedding), bronze snapshots

---

### Issue 11
**Phase boundary score wrong when exact boundary snapshot is missing (2nd innings)**
🔴 Open — deferred

In the 2nd innings of event 11963258, data is missing for overs 14.4–14.5. A snapshot exists at 15.1 (score=139, wickets=5, ball_window=`[0,4,0,2,0,0]`). Since ball 15.1 scored 0 runs, end-of-over-14 score = 139. The phase code does not do this inference — it uses the last captured snapshot as the phase boundary, so Middle (7-15) ends at 14.3 score rather than 139.

- **Proposed fix**: When the exact `X.0` snapshot is missing, find the first snapshot of over X+1, then subtract that ball's run value (`ball_window[0]`) to infer the end-of-over score
- **Files**: `views/match_analysis.py` → `build_phases()`

---

### Issue 12
**Last-ball-of-innings-1 snapshots mislabeled as innings 2**
✅ Fixed in display layer

At the very last ball of innings 1, the BetsAPI sets S3 (target) before PG resets. The silver parser detects S3 → assigns `innings=2`. These rows have over=19.x with score=155 (inn1 total) and showed up in `inn2_rows`, causing:
- Death phase 2nd innings start showing `155/8` instead of the true inn2 start score
- Chase Analysis RRR showing 42 instead of 6 (because target was also wrong — see Issue 13)

- **Fix applied**: `match_analysis.py` now filters `inn2_rows` to exclude any row where `score == inn1_runs AND wickets == inn1_wkts`
- **Root fix still needed**: `bronze_to_silver.py` innings detection should handle this transition edge case

---

### Issue 13
**`score_summary_events` is home-first, not innings-first**
✅ Fixed in display layer

`score_summary_events` stores scores in home/away order (raw BetsAPI `ss` field). `match_analysis.py` was treating `parts[0]` as innings 1 and `parts[1]` as innings 2. For matches where the away team batted first, this reverses inn1/inn2 — wrong target, wrong phase ranges, RRR showed 42 instead of 6 (event 11963258).

- **Fix applied**: `match_analysis.py` now compares both scores against the last captured snapshot of each innings and assigns whichever pairing minimises the total distance
- **Permanent fix**: Issue 3 — write `batting_first_team_name` in `gold_rebuild.py` so this is deterministic, not a heuristic

---

### Issue 14
**Win% in Chase Analysis — formula explained**
📖 Documentation

Win% is the implied probability from Bet365 decimal odds for the batting (chasing) team at each snapshot.

```
Win% = (1 / batting_team_odds) × 100
```

| Odds | Win% |
|------|------|
| 1.25 | 80% (heavy favourite) |
| 2.00 | 50% (evens) |
| 4.00 | 25% (underdog) |

Odds come from the `batting_team_odds` field in each gold tracker row, captured from the `/v1/bet365/event` market stream during the live match. Rows where the market was suspended or not captured show `—`.

---

## Historical Fixes

These were fixed and are recorded for reference only.

| # | Title | Root cause | Fix |
|---|-------|------------|-----|
| H1 | ML page — Event ID missing | Gold tracker didn't store `event_id`; `t.get("event_id")` returned None | `_dl_tracker()` extracts event_id from blob path; `gold_rebuild.py` writes it into the tracker |
| H2 | ML page — Match name truncated | `views/ml.py` applied `[:50]` | Removed truncation |
| H3 | `ended/view` — Final score wrong | Score came from silver snapshots, not authoritative BetsAPI event result | `gold_rebuild.py` calls `/v1/event/view`; `score_summary_events` is the single source of truth |
| H4 | `ended/view` — Win label wrong (Defended vs Chase Won) | Same as H3 — wrong score led to wrong win type | Same as H3 |
| H5 | Scores missing overs (e.g. "203" instead of "203/7(20)") | `score_summary_events` stored score without overs | `gold_rebuild.py` reads `ss` from BetsAPI event/view which includes overs |
| H6 | `bronze_capture_ended_event_view()` timing out | Scanned thousands of bronze inplay_snapshot blobs to collect event IDs | Now scans `gold/cricket/innings_tracker/` — much smaller set |
| H7 | `payload.get("results")` always returned None | `call_betsapi()` wraps response — `results` is inside `response.body` | All callers now use `extract_results(payload)` |
| H8 | Double-swap bug — scores in wrong order | `gold_rebuild.py` swapped to batting-first AND `bronze_discover_cricket_ended()` swapped again | `gold_rebuild.py` stores home-first (raw). Display layer swaps once only |
| H9 | `func-ramanuj-display` all endpoints 404 after deploy | Deploy ran from `cricket_ingestion` directory and published to the display app | Redeployed from `cricket_display` directory |

---

## Backlog (Low Priority)

- **Hypothesis / ML page scores**: Verify overs format shows correctly once Issue 1 (full gold rebuild) is done.
- **Stadium column on `ended/view`**: Added but needs visual check — some events may show `—` if `stadium_data` was never written to tracker.
- **6 events with no bronze master data**: ~6 of 80 events returned no result from `/v1/event/view` (possibly too old). These will always show scores without overs unless manually patched.
