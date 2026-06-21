# Pipeline Queue Design

## The Problem We Are Solving

Every night the `pl_build_ended_match` pipeline runs. Its first step (`index_new_snapshots`) 
scans the entire bronze container — over 300,000 blobs — just to find the few thousand that 
arrived since the last run. This scan takes 10–15 minutes and grows longer every week as 
bronze accumulates more data.

The scan is slow because:
- Azure Blob Storage has no server-side "give me blobs newer than date X" filter
- The pipeline must list every blob, page by page (5,000 per API call), and check dates in memory
- With 300k+ blobs that means 60+ API round-trips before any real work starts

---

## Current Architecture (the messy one)

### What the ingestion function does

Every 5 seconds `capture_cricket_inplay_snapshot` calls 9 BetsAPI endpoints and writes the 
results to bronze:

```
bronze/
  betsapi/
    inplay_snapshot/
      sport_id=3/
        event_id=12035355/
          fi=196090570/
            snapshot_id=20260619T003000Z__1-1-0/
              manifest.json
              api_inplay_event_list.json
              api_live_market_odds.json
              api_event_odds.json
              api_event_view.json
              ... (10 files total)
```

The ingestion function then stops. It writes nothing else.

### What the pipeline does every morning

```
Step 1: index_new_snapshots  (10-15 min)
  → Lists ALL 300,000+ blobs in bronze
  → Filters to those modified after the last watermark
  → Writes a single file: landing/{run_id}/index.json
     (a list of paths to the new manifests)

Step 2: bronze_to_silver  (15 min)
  → Reads index.json
  → For each path in the index, downloads the 10 bronze files
  → Parses them and writes structured silver data
  → Writes event complete markers to silver/control/complete/

Step 3: silver_to_gold  (5 min)
  → Reads silver data
  → Rebuilds innings tracker JSON files in gold

Step 4: update_watermark  (instant)
  → Writes landing/control/watermarks.json with today's timestamp

Step 5 (failure only): cleanup_landing
  → Deletes landing/{run_id}/index.json
  → Watermark is unchanged so next run re-scans from old cutoff
```

### What "landing" and "watermark" mean right now

| File | What it is | Problem with the name |
|---|---|---|
| `landing/{run_id}/index.json` | A list of paths to bronze blobs that need processing | "landing" sounds like a storage layer, not a queue. "index" sounds like a database index |
| `landing/control/watermarks.json` | Timestamp of the last successful pipeline run | "watermarks" is a data engineering term that isn't obvious |

### Why this is fragile

- The watermark and the work list (`index.json`) are in the same `landing/` container, which is confusing
- The `run_id` folder structure means old empty folders pile up after each run
- If the pipeline never ran before, the scan covers all of bronze history (31,468 manifests in the first run = 800 seconds)
- "landing zone" is an overloaded term — it usually means a staging area for raw incoming data, not a pipeline queue

---

## Proposed Architecture (the clear one)

### Core idea

**The ingestion function already knows what it just wrote. Let it tell the pipeline.**

Instead of scanning bronze every morning, we make the ingestion function leave a tiny 
notification in a queue every time it captures a snapshot. The pipeline then just reads 
the queue — no bronze scan needed.

### New folder structure

```
bronze/                          ← permanent raw data (unchanged)
  betsapi/inplay_snapshot/...

silver/                          ← structured parsed data (unchanged)
  event_id=12035355/...

gold/                            ← final tracker files (unchanged)
  cricket/innings_tracker/...

process-queue/                   ← NEW: pipeline work queue
  pending/                       ← snapshots waiting to be processed
    event_id=12035355/
      20260619T003000Z.json      ← one tiny file per snapshot
    event_id=11658817/
      20260618T140000Z.json
      20260618T140500Z.json
  last-successful-run.json       ← replaces watermarks.json
```

### What each file means

**`process-queue/pending/event_id={eid}/{snapshot_id}.json`**

Written by the ingestion function immediately after saving the bronze snapshot.
Contains just enough to find the bronze data:

```json
{
  "event_id": "12035355",
  "snapshot_id": "20260619T003000Z__1-1-0",
  "manifest_path": "betsapi/inplay_snapshot/sport_id=3/event_id=12035355/fi=196090570/snapshot_id=20260619T003000Z__1-1-0/manifest.json",
  "captured_at_utc": "2026-06-19T00:30:00Z"
}
```

Size: ~300 bytes. Thousands of these cost pennies in storage.

**`process-queue/last-successful-run.json`**

Written by the pipeline only after every step succeeds:

```json
{
  "completed_at_utc": "2026-06-20T01:45:00Z",
  "run_id": "459337b4-7b0a-40b1-900d-4f4ec3dd1979",
  "snapshots_processed": 3118
}
```

### New pipeline flow

```
Step 1: read-pending-queue  (seconds, not minutes)
  → Lists process-queue/pending/ (only new small files, not all of bronze)
  → Skips events whose silver complete marker already exists
  → Skips events still active (snapshot < 60 min ago)
  → Result: a list of {event_id, manifest_path} for this run

Step 2: bronze-to-silver  (15 min, same as today)
  → Reads each manifest from bronze using paths from Step 1
  → Parses and writes silver data
  → Writes event complete markers

Step 3: silver-to-gold  (5 min, same as today)
  → Reads silver, rebuilds gold innings tracker

Step 4: delete-processed-markers + write-last-run  (instant)
  → Deletes process-queue/pending/event_id={eid}/*.json for every event that completed
  → Writes process-queue/last-successful-run.json

Step 5 (failure only): do nothing
  → Pending markers stay in place
  → Next run picks up the same set of snapshots automatically
  → No cleanup activity needed, no watermark to reset
```

### Why failure recovery is simpler

**Current model (watermark + landing index):**
```
Run fails at bronze_to_silver
  → cleanup_landing deletes landing/{run_id}/index.json  ← need explicit cleanup
  → watermark.json untouched                             ← next run re-scans from old date
  → Next run: re-scan 300k blobs again to rebuild the index ← slow again
```

**New model (pending markers):**
```
Run fails at bronze-to-silver
  → pending markers stay exactly where they are          ← nothing to clean up
  → Next run: list pending/ → same files are there → retry the same work
  → No scan, no watermark reset, no cleanup activity
```

The pending markers act as a self-healing queue. Failure means "stop here", not "undo and restart".

---

## Side-by-side comparison

| | Current | Proposed |
|---|---|---|
| **Who builds the work list?** | Pipeline (scan bronze every run) | Ingestion function (writes markers as it captures) |
| **How long does step 1 take?** | 10–15 min (grows over time) | Seconds (list only new markers) |
| **What happens on failure?** | Must delete index.json + leave watermark alone | Do nothing — markers stay, retry automatically |
| **What is "landing" used for?** | Storing the run's index file | Not needed — replaced by process-queue |
| **What is the watermark?** | Timestamp used to filter bronze | Not needed — markers ARE the queue |
| **First run cost?** | 800 seconds (scan entire bronze history) | Must backfill pending markers once (one-time migration) |
| **Naming clarity** | `landing/`, `watermarks.json` — confusing | `process-queue/pending/`, `last-successful-run.json` — clear |

---

## Migration plan (one-time)

The pending markers only exist from the date we deploy the change forward. 
Older bronze snapshots (before the change) have no markers. For those:

**Option A — Ignore old data, start fresh**
Set `last-successful-run.json` to today. Old matches without silver data can be 
backfilled using `pl_backfill` (which bypasses the queue and scans bronze directly 
for the specific event_id).

**Option B — One-time backfill of pending markers**
Run a one-time script that scans bronze once, writes a pending marker for every 
existing manifest that doesn't already have a silver complete marker. After that 
the pipeline handles everything via the queue.

Option A is simpler. Option B gives a full queue-based view from the start.

---

### Option B in detail — `migrate_to_process_queue.py`

**How it decides what to write:**

The answer is already in storage: if `silver/control/complete/event_id={eid}.json` 
exists, the event is done — no marker needed. If it doesn't exist, write pending 
markers for all its snapshots so the new pipeline picks them up.

```
1. Scan bronze once (betsapi/inplay_snapshot/sport_id=3/)
   → Collect every manifest.json path
   → Group by event_id: { eid: [(snapshot_id, fi, manifest_path), ...] }

2. For each event_id:
   → Check if silver/control/complete/event_id={eid}.json exists
   → If YES: skip (already processed, pipeline will skip it too)
   → If NO:  write one pending marker per snapshot to
             process-queue/pending/event_id={eid}/{snapshot_id}.json

3. Print summary:
   → N events already complete (skipped)
   → N events needing processing (markers written)
   → N total markers written
```

**Script:**

```python
# migrate_to_process_queue.py
# Run once before switching ADF to the new queue-based pipeline.

from collections import defaultdict
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import json, os

bronze        = BlobServiceClient(...).get_container_client("bronze")
silver        = BlobServiceClient(...).get_container_client("silver")
process_queue = BlobServiceClient(...).get_container_client("process-queue")

# Step 1: collect all manifests from bronze (one-time scan, ~800s)
by_event = defaultdict(list)
for blob in bronze.list_blobs(name_starts_with="betsapi/inplay_snapshot/sport_id=3/"):
    if not blob.name.endswith("/manifest.json"):
        continue
    parts = blob.name.split("/")
    eid = next((p[9:]  for p in parts if p.startswith("event_id=")), None)
    fi  = next((p[3:]  for p in parts if p.startswith("fi=")),       None)
    sid = next((p[12:] for p in parts if p.startswith("snapshot_id=")), None)
    if eid and sid:
        by_event[eid].append((sid, fi, blob.name))

print(f"Found {len(by_event)} events, {sum(len(v) for v in by_event.values())} total snapshots")

# Step 2: write markers only for events without a silver complete marker
skipped = markers_written = 0
for eid, snapshots in by_event.items():
    complete_path = f"control/complete/event_id={eid}.json"
    try:
        silver.get_blob_client(complete_path).get_blob_properties()
        skipped += 1
        continue  # already done — pipeline will skip anyway
    except ResourceNotFoundError:
        pass

    for sid, fi, manifest_path in snapshots:
        marker = {
            "event_id":       eid,
            "snapshot_id":    sid,
            "manifest_path":  manifest_path,
            "captured_at_utc": None,   # unknown for old blobs
            "migrated":       True,    # flag so we can distinguish migrated vs live markers
        }
        process_queue.get_blob_client(
            f"pending/event_id={eid}/{sid}.json"
        ).upload_blob(json.dumps(marker).encode(), overwrite=True)
        markers_written += 1

print(f"Events skipped (already complete): {skipped}")
print(f"Events needing processing: {len(by_event) - skipped}")
print(f"Pending markers written: {markers_written}")
```

**Why this is safe:**

- Events with complete markers are skipped by the migration AND by `read_pending_queue` — no double processing
- Events without complete markers get markers for ALL their snapshots. `bronze_to_silver` silver writes are idempotent so reprocessing partially-done snapshots is harmless
- Script can be run multiple times safely — all writes use `overwrite=True`
- The `"migrated": true` flag lets you distinguish old markers from live ones in logs

**One-time cost:** same 800s scan as the current first run — but it only ever runs once.

**Deployment sequence:**

```
1. Deploy new ingestion function  → starts writing markers for new snapshots from here forward
2. Run migrate_to_process_queue.py → writes markers for all unprocessed historical events
3. Switch ADF pipelines           → read from process-queue instead of scanning bronze
4. Delete landing container       → no longer needed
```

Steps 1 and 2 can overlap safely — ingestion writes to per-snapshot paths, migration writes to different per-snapshot paths, no collision possible.

---

## Changes needed

### 1. Ingestion function (`capture_cricket_inplay_snapshot`)

After writing the bronze snapshot bundle, add one extra blob write:

```python
# Write a pending marker so the pipeline queue knows about this snapshot
queue_marker = {
    "event_id": event_id,
    "snapshot_id": snapshot_id,
    "manifest_path": f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/manifest.json",
    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
}
process_queue.get_blob_client(f"pending/event_id={event_id}/{snapshot_id}.json").upload_blob(
    json.dumps(queue_marker).encode(), overwrite=True
)
```

Cost: one extra blob write per snapshot. The ingestion function already makes 9 API calls 
per snapshot; one more blob write is negligible.

### 2. Replace `index_new_snapshots` with `read-pending-queue`

New script (`read_pending_queue.py`) — replaces 150 lines of scan logic with:

```python
# List pending markers (fast — only new small files)
pending = {}
for blob in process_queue.list_blobs(name_starts_with="pending/"):
    data = json.loads(process_queue.get_blob_client(blob.name).download_blob().readall())
    eid = data["event_id"]
    pending.setdefault(eid, []).append(data)

# Skip events already complete
# Skip events still active (captured < 60 min ago)
# Write work list to process-queue/in-progress/{run_id}.json
```

### 3. `bronze-to-silver` reads from in-progress list instead of `landing/index.json`

Minimal change — just change where it reads the work list from.

### 4. On success: delete processed markers

```python
# After all events processed, delete their pending markers
for eid in succeeded_events:
    for blob in process_queue.list_blobs(name_starts_with=f"pending/event_id={eid}/"):
        process_queue.get_blob_client(blob.name).delete_blob()
```

### 5. Remove cleanup_landing and fail_pipeline activities from ADF

Not needed anymore — failure recovery is automatic via marker persistence.

---

## Files that change

| File | Change |
|---|---|
| `src/functions/cricket_ingestion/capture_cricket_inplay_snapshot.py` | Add pending marker write |
| `infra/7.databricks/lib/landing_index.py` | Replace with `process_queue.py` |
| `infra/4.azure-batch/scripts/index_new_snapshots.py` | Replace with `read_pending_queue.py` |
| `infra/7.databricks/notebooks/index_new_snapshots.py` | Replace with `read_pending_queue.py` |
| `infra/4.azure-batch/scripts/bronze_to_silver.py` | Change where it reads the work list |
| `infra/7.databricks/notebooks/bronze_to_silver.py` | Same |
| `infra/8.adf-config/main.tf` | Remove cleanup_landing + fail_pipeline, rename index activity |

## Files that disappear

| File | Why |
|---|---|
| `landing_index.py` | Replaced by `process_queue.py` |
| `update_watermark.py` | No longer needed |
| `infra/4.azure-batch/scripts/update_watermark.py` | No longer needed |
| `infra/7.databricks/notebooks/update_watermark.py` | No longer needed |

## New storage container

A new `process-queue` container in the storage account (or a subfolder in `landing` 
if we don't want a new container). The `landing` container becomes unused and can be deleted.
