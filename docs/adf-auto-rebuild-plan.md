# ADF Auto-Rebuild Plan for Ended Innings Tracker

## The Problem

When a cricket match ends, the system needs to rebuild `innings_1_from_silver.json` — a
gold-layer file that aggregates all historical silver snapshots into the full innings
timeline. Without this file a match never appears in `/api/ended/view`.

Currently (as of May 2026) this rebuild is triggered by:

1. **Manual HTTP call** — `GET /api/mgmt/innings/{event_id}/rebuild` — operator must know
   which match just ended and call it explicitly.
2. **Function App timer** (`auto_rebuild_ended_innings_tracker`, every 10 minutes) — detects
   matches with `innings_1.json` but no `innings_1_from_silver.json` and calls
   `_rebuild_innings_core()` internally.

The timer approach works but has a hard ceiling that makes it unreliable for large matches.

---

## Why the Function App Timer Is Not the Right Long-Term Solution

### The 10-minute function timeout

Azure Functions has a configurable execution timeout set in `host.json`:

```json
{
  "version": "2.0",
  "functionTimeout": "00:10:00"
}
```

This is already set to the maximum we run (10 minutes). A single call to
`_rebuild_innings_core()` must scan every bronze manifest for the event, reconstruct
silver snapshot paths, and read each snapshot file one by one. The time this takes
scales directly with the number of snapshots:

| Match type | Approx snapshots | Rebuild time |
|------------|-----------------|--------------|
| T20 (IPL)  | ~800–1,000      | ~2–4 min     |
| ODI (50-over) | ~3,000–5,000 | ~6–10 min    |
| Test match | 10,000–50,000+  | exceeds 10 min → **always times out** |

### How the current timer works around this

The timer caps at **2 rebuilds per run** to try to stay inside the 10-minute window.
For two simultaneous T20 matches this is fine. But:

- Two ODIs in the same run will almost certainly time out, leaving one half-rebuilt.
- A Test match will always time out regardless of how many we cap per run.
- If the function crashes mid-rebuild, there is no retry — the match stays missing
  from the ended index until the next timer run.
- The timer and the HTTP endpoint share the same function host process, so a timeout
  in the timer can affect other concurrent function executions.

---

## The Right Solution: ADF Pipeline

Azure Data Factory (ADF) is already used in this project
(`batch_silver_reprocess` pipeline calls `view_admin_reprocess_silver`). It is the
correct orchestration layer for this job because:

- **No orchestration timeout** — ADF schedules individual Web Activity calls; each
  call gets its own isolated Function execution with its own 10-minute window.
- **Built-in retry** — configure each Web Activity with 2–3 retries on failure.
- **Parallelism control** — ForEach activity with `batchCount` lets you run N rebuilds
  in parallel without them competing for a single function host timeout.
- **Monitoring and alerting** — ADF pipeline runs appear in Azure Monitor; failures
  send alerts without any custom logging code.
- **No code coupling** — the ADF pipeline calls the existing HTTP endpoints; no new
  Function App logic is needed.

---

## Implementation Plan

### Step 1 — Add a rebuild-candidates endpoint

Add a new HTTP route to `views.py` and `function_app.py`:

```
GET /api/mgmt/innings/rebuild-candidates
```

Returns a JSON list of event IDs that have `innings_1.json` but not
`innings_1_from_silver.json`, are not currently live, and are in an allowed league:

```json
{
  "candidates": ["11658827", "11658853"],
  "generated_at_utc": "2026-05-09T10:00:00"
}
```

This is a fast call — it only lists blobs, no downloads.

### Step 2 — Create the ADF pipeline

Pipeline name: `auto_rebuild_ended_innings`

```
Trigger: Schedule — every 15 minutes

Activity 1: Web (GET /api/mgmt/innings/rebuild-candidates)
  → output: candidates array

Activity 2: ForEach over candidates
  batchCount: 3  (3 parallel rebuilds)
  Inner activity: Web (GET /api/mgmt/innings/{event_id}/rebuild)
    timeout: 00:10:00
    retry: 2
    retryInterval: 00:02:00
```

### Step 3 — Remove the Function App timer

Once the ADF pipeline is running and verified, remove `auto_rebuild_ended_innings_tracker`
from `function_app.py` and `auto_rebuild_ended_innings()` from `views.py`.

The manual HTTP endpoint (`/api/mgmt/innings/{event_id}/rebuild`) should be kept — it
is useful for one-off operator-triggered rebuilds.

---

## Why Test Matches Need ADF Especially

A Test match runs over 5 days and generates 50,000+ bronze snapshots. The rebuild for
a single Test match will always exceed the 10-minute Function App timeout. With ADF:

- The Web Activity timeout can be set to 30–60 minutes per match independently.
- ADF does not share a process with any other Function App executions.
- If a Test match rebuild fails partway, ADF retries from the start automatically.

---

## Current State (May 2026)

- The Function App timer (`auto_rebuild_ended_innings_tracker`, every 10 min) is live
  and being tested. It handles T20 and most ODI matches correctly.
- The 2-per-run cap means if more than 2 matches end simultaneously, the extras are
  picked up on the next timer run (10 minutes later).
- Migrate to ADF once the current timer approach has been validated over a few days of
  live matches.
