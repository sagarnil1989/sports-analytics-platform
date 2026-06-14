# Issues Log

---

## FIXED (Historical)

1. **ML page — Event ID missing**
   ROOT CAUSE: gold tracker didn't store `event_id`; `t.get("event_id")` returned None.
   FIX: `_dl_tracker()` in ML notebook extracts event_id from blob path; `gold_rebuild.py` writes event_id into tracker dict.

2. **ML page — Match name truncated**
   ROOT CAUSE: `views/ml.py` applied `[:50]` truncation.
   FIX: Removed truncation.

3. **ended/view — Final score wrong (e.g. 203 shown as 201)**
   ROOT CAUSE: gold tracker score came from silver snapshots, not the authoritative BetsAPI event result.
   FIX: `gold_rebuild.py` now calls `/v1/event/view` for authoritative score with overs; `score_summary_events` is the single source of truth.

4. **ended/view — Win label wrong (Defended vs Chase Won)**
   ROOT CAUSE: same as #3 — wrong score led to wrong win type calculation.
   FIX: Same as #3.

5. **Scores missing overs (e.g. "203" instead of "203/7(20)")**
   ROOT CAUSE: `score_summary_events` stored score without overs.
   FIX: `gold_rebuild.py` now reads `ss` field from BetsAPI event/view which includes overs, e.g. `"163/9(20),167/6(19.5)"`.

6. **`bronze_capture_ended_event_view()` timing out (10 min limit)**
   ROOT CAUSE: scanned all bronze inplay_snapshot blobs (thousands) to collect event IDs.
   FIX: Now scans `gold/cricket/innings_tracker/` for `innings_1_from_silver.json` — much smaller set.

7. **`payload.get("results")` always returned None**
   ROOT CAUSE: `call_betsapi()` wraps response in `{"request":{}, "response":{"body":{}}}` — `results` is inside `response.body`.
   FIX: All callers now use `extract_results(payload)`.

8. **Double-swap bug — scores shown in wrong order**
   ROOT CAUSE: `gold_rebuild.py` swapped to batting-first AND `bronze_discover_cricket_ended()` swapped again — double-swap → wrong result.
   FIX: `gold_rebuild.py` stores home-first (raw from BetsAPI `ss`). Only display layer swaps once.

9. **`func-ramanuj-display` deployed with wrong code (all endpoints 404)**
   ROOT CAUSE: deploy command `cd cricket_ingestion && func publish func-ramanuj-display` published ingestion code to display app.
   FIX: Redeployed `func-ramanuj-display` from `cricket_display` directory.

---

## OPEN — Pending Fix

### Issue 1 — Gold rebuild not run for all events (only 1 of 80 done)
**Status**: Open
**Description**: Gold tracker rebuild with authoritative scores (bronze master + overs) was run only for event `11963258` as a test. The remaining ~79 ended events still have old tracker data — scores may be wrong, overs may be missing, stadium/league/home/away may come from silver instead of master.
**Impact**: `ended/view`, hypothesis pages, ML pages all show stale data for ~79 events.
**Fix needed**: Run `gold_rebuild_ended_matches()` for all events (no `event_id` filter). Must first validate event 11963258 is correct, then run all.
**Blocker**: `trigger_build_ended_match` ADF trigger is currently STOPPED. Must re-enable after fixes are validated.

---

### Issue 2 — Score ordering fallback when `batting_team` rows are absent
**Status**: Open
**Description**: `bronze_discover_cricket_ended()` determines batting-first order by scanning `tracker["rows"]` for the first row where `innings == 1` and reading `batting_team`. If the gold tracker has no `rows` (e.g. silver never captured live data for that event), `inn1_bat` stays None and the swap never happens — home team score always shown first regardless of who batted first.
**Impact**: Any event where silver rows are sparse will show scores in wrong order on `ended/view`.
**Fix needed**: Add a secondary fallback — if rows are absent, try to infer batting order from the `ss` field structure or from a dedicated `batting_first_team` field written by `gold_rebuild.py`.
**Files**: `prematch_capture.py` → `bronze_discover_cricket_ended()`, `gold_rebuild.py`

---

### Issue 3 — `gold_rebuild.py` does not write a `batting_first_team_name` field
**Status**: Open
**Description**: The gold tracker has no explicit field recording which team batted first. The batting order is inferred at display time by scanning `rows`, which is fragile (see Issue 2). `gold_rebuild.py` knows the BetsAPI `ss` field (home-first) and knows home/away team names — it could write `batting_first_team_name` explicitly by comparing the 1st-innings score with the rows or with a dedicated field.
**Impact**: Makes Issue 2's fallback impossible without this field.
**Fix needed**: In `gold_rebuild.py`, determine which team batted first and write `"batting_first_team_name"` into the tracker JSON.
**Files**: `infra/7.databricks/lib/gold_rebuild.py`

---

### Issue 4 — Phase breakdown ball stats wrong (Section 7, match_data_explorer)
**Status**: Open
**Description**: 4s, 6s, and dot balls shown in Section 7 of `match_data_explorer` are wrong. The `ball_window` comparison logic is unreliable — the rolling 6-ball window in the `PG` field spans over boundaries and cannot be used directly to count events per phase.
**Impact**: Section 7 of the explorer notebook shows incorrect phase statistics.
**Fix needed**: Reconstruct per-ball events from consecutive snapshot diffs rather than reading the window at a single snapshot.
**Files**: match_data_explorer notebook

---

### Issue 5 — `trigger_build_ended_match` ADF trigger is stopped
**Status**: Open (intentional hold)
**Description**: Trigger was manually stopped on 2026-06-10 to prevent incorrect gold rebuilds from running while code issues are being fixed.
**Action needed**: Re-enable trigger only after Issues 1–3 above are validated and the full gold rebuild has been run cleanly.
**Command to re-enable**:
```bash
az datafactory trigger start --factory-name adf-ramanuj --resource-group rg-ramanuj --name trigger_build_ended_match
```

---

### Issue 6 — `capture_ended_event_view` logs at `logging.info` level are filtered
**Status**: Partially fixed
**Description**: Per-event API response logs were upgraded to `logging.warning` to ensure App Insights captures them. However, all other diagnostic logs in the function still use `logging.info` and may be filtered by default App Insights sampling.
**Impact**: Debugging pipeline failures requires checking Databricks logs rather than App Insights.
**Fix needed**: Review and upgrade key diagnostic log lines to `logging.warning` in `prematch_capture.py`.

---

## UI / Display Issues

### Issue 7 — Home page shows "Live Matches" section — remove it
**Status**: FIXED (2026-06-11)
**Description**: `https://func-ramanuj-display.azurewebsites.net/api/home` showed a Live Matches section. Removed along with all `/api/matches/...` live routes as part of the decision to remove live match tracking entirely from the display app.
**Files changed**: `views/home.py`, `function_app.py`, `views/__init__.py`

---

### Issue 8 — Home page shows "Innings Tracker Analytics" section — remove it
**Status**: FIXED (2026-06-11)
**Description**: Removed from home page and `/api/innings-tracker` route removed from `function_app.py`. The per-match innings tracker (`/api/matches/{event_id}/innings-tracker/view`) is kept — it reads gold files and is still useful for ended matches.
**Files changed**: `views/home.py`, `function_app.py`, `views/__init__.py`

---

### Issue 9 — ADF pipeline `pl_build_prematch_pages` not yet created
**Status**: Open
**Description**: Prematch capture now runs once daily (06:00–06:10 UTC) and writes bronze only. The gold prematch index (`gold/cricket/prematch/latest/index.json`) that the `/api/prematch/view` display page reads is no longer updated by the Function App. A new ADF pipeline `pl_build_prematch_pages` needs to be created in Databricks to read `bronze/betsapi/prematch_snapshot/` and write `gold/cricket/prematch/`. Reference logic is in `capture_prematch.py` — `gold_build_cricket_prematch_pages()` and `gold_write_prematch_indexes()`.
**Impact**: `/api/prematch/view` shows stale data until the ADF pipeline is created and run.
**Fix needed**: Create Databricks notebook `prematch_gold_build.py` and ADF pipeline `pl_build_prematch_pages` triggered daily at 07:00 UTC (after capture completes at 06:10).

---

## Backlog (Known but Low Priority)

- **Hypothesis page scores**: Verify overs format shows correctly after Issue 1 is fixed.
- **ML page scores**: Verify overs format shows correctly after Issue 1 is fixed.
- **Stadium column on `ended/view`**: Added but needs visual check across multiple events — some may show `-` if `stadium_data` was never written to tracker.
- **6 events where bronze master fetch returned no data**: ~6 of 80 events had no result from `/v1/event/view` — possibly too old. These events will always show scores without overs unless manually patched.

