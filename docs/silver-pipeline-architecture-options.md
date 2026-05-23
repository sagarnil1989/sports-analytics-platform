# Silver Pipeline Architecture Options

> **Decision (May 2026): Option A (ADF + Databricks) has been implemented.**
> Silver and gold pipelines run as Databricks notebooks triggered by ADF.
> The Function App now handles bronze capture and HTTP routes only.
> The sections below are preserved for context on why this decision was made.
>
> **Update (May 2026):** The original three-pipeline design (pl_silver_inplay every 30 min,
> pl_gold_match_pages every 1 hr, pl_gold_auto_rebuild every 15 min) has been consolidated
> into two pipelines: `pl_build_ended_match` (daily, two sequential activities) and
> `pl_backfill` (manual). Live match gold pages are out of scope.

---

## Background — Why the Original Approach Had Problems

The silver pipeline (`parse_cricket_bronze_to_silver`) originally ran on an Azure Functions
Consumption plan with a timer trigger every 1 hour. This caused repeated data gaps
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

4. **Recency sort buries old unprocessed snapshots** — manifests were sorted
   newest-first. Old unprocessed snapshots from hours or days ago sat at the bottom of
   the list and were never reached within the time budget.

---

## Option A: ADF + Databricks ✅ Implemented

### How it works

- Function App handles bronze capture only (every 5s, with hash-based dedup) and HTTP API routes.
- ADF triggers two pipelines, each with two sequential Databricks notebook activities:
  - **`pl_build_ended_match`** — daily at 02:00 CET
    - Activity 1: `silver_build_ended_match` — parse bronze → silver for matches quiet >1 hour
    - Activity 2: `gold_build_ended_match` — rebuild gold from silver (runs only if Activity 1 succeeded)
  - **`pl_backfill`** — manual only, accepts optional `event_id` parameter
    - Activity 1: `silver_backfill` — silver backfill for one match or all quiet matches
    - Activity 2: `gold_backfill` — gold rebuild for the same scope (runs only if Activity 1 succeeded)
- Live match gold pages are out of scope — gold contains only ended match data.

### Why it was chosen

- No timeout ceiling — Databricks jobs run as long as needed.
- ADF schedule triggers are guaranteed — never dropped by cold starts.
- Quiet-match detection (>60 min since last bronze snapshot) prevents touching active matches.
- Processed_snapshot markers prevent double-processing — no watermark needed.
- Gold reads from silver only — no bronze access in gold build.
- Single daily run keeps costs low vs. frequent polling.

### Infrastructure

- Module `7.databricks`: Databricks Premium workspace, secret scope, notebooks, DBFS source files.
- Module `8.adf-config`: ADF Key Vault linked service, Databricks linked service, pipelines, triggers.
- Databricks authenticates to ADF via a PAT stored in Key Vault (`kv-ramanuj/databricks-pat`).
- Cluster: single-node `Standard_DS4_v2` (8 vCPUs, 28 GB RAM) with `spark.databricks.cluster.profile=singleNode`.

### Cost

~€80–150/month for Databricks job clusters. Job clusters spin up per run (~2–3 min startup)
and shut down when done — no always-on cost.

---

## Option B: ADF Orchestrating the Existing Function (Not chosen)

Remove the timer trigger from the silver pipeline. Convert it to an HTTP-triggered
function. ADF calls it on a schedule with a 30-minute Web Activity timeout.

### Why not chosen

- Still single-threaded sequential processing inside the function.
- 30-minute timeout helps but does not eliminate timeout risk for large match backlogs.
- Adds ADF-to-Function coupling without solving the core reliability problem.

---

## Option C: Blob Trigger + Event Grid (Future consideration)

Instead of a timer that wakes up periodically and processes everything in bulk, the silver
pipeline fires once per bronze manifest, the moment it is written to storage.

### How it would work

```
Bronze writes manifest.json
    ↓
ADLS Gen2 emits blob-created event via Event Grid
    ↓
silver_on_manifest_written() fires — processes ONE snapshot (~2-5 seconds)
    ↓
Done ✓
```

### Why deferred

- No backlog handling — old manifests already in bronze do not fire triggers.
- Event Grid subscription adds setup complexity (HNS requires Event Grid, not polling trigger).
- Current ADF + Databricks approach handles the volume adequately for T20/ODI.
- Revisit when Test matches are added (50,000+ snapshots per match) or if real-time silver
  latency becomes a product requirement.

---

## Why Event Grid Is Required for ADLS Gen2 Blob Triggers

ADLS Gen2 has **Hierarchical Namespace (HNS)** enabled. The standard Azure Functions blob
trigger uses polling against the flat Blob Storage API and behaves unreliably on HNS
containers. Event Grid hooks directly into ADLS Gen2's own event system and is the only
reliable way to trigger on new blobs in an HNS-enabled storage account.

| | No Event Grid (polling) | With Event Grid (push) |
|---|---|---|
| Trigger mechanism | Runtime polls storage every 30–60s | ADLS pushes event instantly on write |
| Latency | 30–60 seconds | ~1 second |
| Works on ADLS Gen2 HNS | Unreliable / not guaranteed | Yes — natively supported |
| Missed blobs under load | Possible | No — every write fires exactly one event |
| Cost | €0 | ~€0.18/mo |
