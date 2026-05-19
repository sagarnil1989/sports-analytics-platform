# Silver Pipeline Architecture Options

## Background — Why the Current Approach Has Problems

The silver pipeline (`parse_cricket_bronze_to_silver`) runs on an Azure Functions
Consumption plan with a timer trigger every 1 hour. This has caused repeated data gaps
in the innings tracker for ended matches.

### Root causes identified

1. **Consumption plan timer unreliability** — the Consumption plan scales to zero when
   idle. When the timer fires, the instance may need a cold start (30–60 seconds). Under
   load, timers can be delayed or dropped entirely. During IPL with 9 concurrent matches
   running on May 8 2026, the silver pipeline was completely absent for 73 minutes —
   zero snapshots processed — despite the timer being set to every 10 seconds at the time.

2. **10-minute function timeout ceiling** — Azure Functions Consumption plan has a hard
   maximum of 10 minutes per execution. A single match with 2,000+ unprocessed snapshots
   takes 17+ minutes to process, so it always times out mid-run leaving a gap.

3. **Sequential per-blob marker checks** — the old approach checked each bronze manifest
   against storage individually (`blob_exists()` per manifest). With 10,000+ total
   manifests accumulated across all past matches, this consumed the entire 9-minute
   budget on network round-trips before reaching unprocessed old snapshots.

4. **Recency sort buries old unprocessed snapshots** — manifests are sorted
   newest-first. Old unprocessed snapshots from hours or days ago sit at the bottom of
   the list and are never reached within the time budget.

### What was fixed in May 2026 (partial fixes)

- Removed the per-snapshot `blob_exists()` loop — replaced with one bulk listing of all
  silver markers into a set, then a set-difference to find unprocessed manifests.
- Added a 9-minute hard budget so the function exits cleanly instead of timing out.
- Changed timer from every 10 seconds to every 1 hour (more reliable on Consumption plan
  for longer-running jobs).
- Removed the arbitrary 200-snapshot limit per run.

These fixes improved reliability but did not eliminate the root problem: a timer-based
bulk processor on a Consumption plan is the wrong architecture for this job.

---

## Option A: ADF + Databricks

### How it works

- Function App handles bronze capture only (every 5s) and HTTP API routes.
- ADF triggers a Databricks job hourly.
- Databricks reads all unprocessed bronze manifests and processes them in parallel using
  Spark, writing silver and gold outputs.

### Pros

- No timeout ceiling — Databricks jobs can run for hours.
- True parallelism — all 2,000 snapshots for one match processed simultaneously.
- Built-in retry, monitoring, and lineage in ADF.
- Auto-scaling clusters.

### Cons

- **Cost** — even a minimal Databricks cluster (1 driver, no workers) costs ~€80–150/mo
  just for 1 hour/day of runtime. More during IPL season with frequent runs.
- **Overkill for the data volume** — 10,000 snapshots/day is small data. Spark cluster
  startup alone takes 2–3 minutes, dominating the runtime for small batches.
- New infrastructure to set up and maintain: Databricks workspace, clusters, notebooks,
  secret scopes.

### Verdict

Right architecture when Test matches are added (50,000+ snapshots per match, true big
data). Premature for current IPL T20/ODI volumes.

---

## Option B: ADF Orchestrating the Existing Function

### How it works

Remove the timer trigger from the silver pipeline. Convert it to an HTTP-triggered
function. ADF calls it on a schedule with a 30-minute Web Activity timeout.

```
ADF Pipeline — hourly schedule trigger (guaranteed, never drops)
  └── Web Activity: POST /api/silver/process
        timeout: 00:30:00
        retry: 2
        retryInterval: 00:05:00
```

The silver processing code in `silver.py` is completely unchanged. ADF just provides a
more reliable trigger with a longer timeout window than the Consumption plan timer.

### What changes

1. Remove `parse_cricket_bronze_to_silver` timer trigger from `function_app.py`.
2. Add an HTTP-triggered function wrapping `silver_parse_bronze_to_silver()`.
3. Create one ADF pipeline: schedule trigger + one Web Activity pointing at the new
   HTTP endpoint with master key auth.

### Pros

- Minimal code change — all silver logic untouched.
- ADF schedule trigger never drops — it is a managed orchestration service, not subject
  to Consumption plan cold starts.
- 30-minute timeout instead of 10 minutes — handles larger backlogs per run.
- ADF built-in retry handles transient failures.
- ADF monitoring dashboard shows success/failure without custom logging.

### Cons

- Still single-threaded sequential processing inside the function — a 2,000-snapshot
  backlog still takes the same wall-clock time, just with more time available.
- Still a batch approach — silver is at most 1 hour behind bronze.

### Cost

ADF charges per activity run. One Web Activity per hour = 720/month.
At €0.001 per run = **~€0.72/month extra**. Essentially free.

---

## Option C: Blob Trigger + Event Grid (recommended long-term)

### The core idea

Instead of a timer that wakes up hourly and processes everything in bulk, the silver
pipeline fires **once per bronze manifest, the moment it is written to storage**.

### How it works

```
Bronze writes manifest.json at 14:00:05
    ↓
ADLS Gen2 emits a blob-created event (milliseconds)
    ↓
Event Grid pushes the event to Azure Functions
    ↓
silver_on_manifest_written() fires — processes ONE snapshot (~2-5 seconds)
    ↓
Done ✓

Bronze writes manifest.json at 14:00:10
    ↓
Event Grid pushes immediately
    ↓
silver_on_manifest_written() fires — processes ONE snapshot
    ↓
Done ✓
```

Every snapshot is processed within seconds of being written. There is no backlog. There
is no timeout risk — processing one snapshot takes 2–5 seconds regardless of how many
total snapshots exist.

### What changes in code

**`function_app.py`** — replace timer trigger with blob trigger:

```python
# Remove:
@app.timer_trigger(schedule="0 0 */1 * * *", ...)
def parse_cricket_bronze_to_silver(timer):
    silver_parse_bronze_to_silver()

# Add:
@app.blob_trigger(
    arg_name="blob",
    path="bronze/betsapi/inplay_snapshot/sport_id=3/{event_id}/{fi}/{snapshot_id}/manifest.json",
    connection="DATA_STORAGE_CONNECTION_STRING"
)
def silver_on_manifest_written(blob: func.InputStream):
    silver_parse_single_manifest(blob.name)
```

**`silver.py`** — extract the per-snapshot logic into a new function
`silver_parse_single_manifest(manifest_path)`. The existing bulk function
`silver_parse_bronze_to_silver()` is kept for one-time backfill runs but no longer
called on a timer.

### What happens to old unprocessed snapshots

Blob triggers only fire for **new** blobs. Old manifests already sitting in bronze
(e.g. 11658824, 11658853) will not fire a trigger — they need a one-time backfill.

For the one-time backfill: the existing ADF `batch_silver_reprocess` pipeline can scan
bronze for manifests with no silver marker and process them. After this runs once, the
blob trigger keeps everything current forever with no further intervention.

### Pros

- No timer — no cold start gaps, no dropped triggers.
- No timeout risk — one snapshot per invocation, always completes in seconds.
- No backlog — silver is always within seconds of bronze during a live match.
- Self-healing — if a snapshot fails, it can be retried individually without affecting
  others.
- Scales automatically with match count — 1 match or 20 matches, each snapshot is
  processed independently.

### Cons

- Old unprocessed snapshots need a one-time backfill run (not automatic).
- Slightly more setup: Event Grid subscription must be wired up.

### Cost

| Component | Purpose | Cost |
|---|---|---|
| Azure Functions Consumption | Runs silver processing | €0 (within 1M free executions/month) |
| Event Grid subscription | Detects new manifest.json blobs on ADLS Gen2 | ~€0.18/mo (300K events/mo at €0.60/million) |
| **Total new cost** | | **~€0.18/mo** |

---

## Why Event Grid is Required for ADLS Gen2

### What is HNS (Hierarchical Namespace)

ADLS Gen2 has **Hierarchical Namespace (HNS)** enabled. This means storage is organized
as a real directory tree rather than a flat list of blobs.

**Without HNS (regular Blob Storage):**
```
bronze/betsapi/inplay_snapshot/sport_id=3/event_id=11658853/manifest.json
```
This looks like a folder path but is just a blob with `/` in its name. There are no real
directories — it is a flat list.

**With HNS (your storage — stramanujdlweu):**
```
bronze/
  └── betsapi/
        └── inplay_snapshot/
              └── sport_id=3/
                    └── event_id=11658853/   ← real directory object
                          └── manifest.json  ← real file
```
Real directories exist at the filesystem level. Deleting a directory deletes everything
inside it atomically. This is why `BlobContainerClient.delete_blobs()` fails with
`DirectoryNotEmpty` on HNS storage — directory entries are real objects that must be
deleted with `DataLakeServiceClient.delete_directory()`.

### Why this matters for blob triggers

The standard Azure Functions blob trigger was built for flat Blob Storage. It uses an
internal polling mechanism against the Blob Storage API, checking for new blobs every
30–60 seconds.

ADLS Gen2 with HNS uses a **different underlying API** (the Data Lake Storage API). The
polling trigger can miss events or behave unpredictably on HNS containers — Microsoft's
own documentation states blob triggers are not guaranteed on ADLS Gen2 without Event
Grid.

### Polling vs Event Grid

| | No Event Grid (polling) | With Event Grid (push) |
|---|---|---|
| Trigger mechanism | Runtime polls storage every 30–60s | ADLS pushes event instantly on write |
| Latency | 30–60 seconds | ~1 second |
| Works on ADLS Gen2 HNS | Unreliable / not guaranteed | Yes — natively supported |
| Missed blobs under load | Possible | No — every write fires exactly one event |
| Cost | €0 | ~€0.18/mo |
| Setup | Just the blob trigger attribute | Blob trigger + one Event Grid subscription |

Event Grid hooks directly into ADLS Gen2's own event system, which speaks the HNS API
natively. It is the only reliable way to trigger on new blobs in an HNS-enabled storage
account.

---

## Comparison Summary

| | Current (timer) | Option B (ADF) | Option C (blob trigger + Event Grid) |
|---|---|---|---|
| **Reliability** | Poor — cold starts, dropped timers | High — ADF guaranteed schedule | Highest — event-driven, no timer |
| **Latency to silver** | Up to 1 hour (+ gaps) | Up to 1 hour | ~1 second |
| **Timeout risk** | Yes — 10 min ceiling | Reduced — 30 min | None — 2-5s per invocation |
| **Backlog handling** | Automatic but slow | Automatic, more time | One-time backfill needed, then none |
| **Code changes** | None | Small | Medium |
| **New infrastructure** | None | ADF pipeline (already have ADF) | Event Grid subscription |
| **Cost change** | €0 | ~€0.72/mo | ~€0.18/mo |
| **Best for** | — | Quick reliable fix | Permanent clean architecture |

---

## Recommendation

- **Immediately:** Option B — wire ADF to call the silver HTTP endpoint. Fixes
  reliability with minimal change, buys time to implement Option C properly.
- **Next sprint:** Option C — blob trigger + Event Grid. Eliminates the timer entirely,
  processes each snapshot as it arrives, no more backlog problems ever.
- **When Test matches are added:** Evaluate Databricks if snapshot counts per match
  exceed ~20,000 and parallel processing becomes necessary.
