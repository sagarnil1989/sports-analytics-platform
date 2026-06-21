# Databricks notebook: read_pending_queue
# Replaces index_new_snapshots. Lists pending markers from process-queue/pending/
# (seconds, not 800s) instead of scanning 300k+ bronze blobs.
# Activity 0 in pl_build_ended_match_databricks and pl_backfill_databricks.
#
# Normal mode  (event_id not set): reads pending markers, skips complete/active events
# Backfill mode (event_id set):    scans bronze directly for that one event

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

import sys, os, json, time, types
from datetime import datetime, timezone, timedelta
from collections import defaultdict

def _load_from_dbfs(module_name, dbfs_path):
    content = dbutils.fs.head(dbfs_path, 500000)
    mod = types.ModuleType(module_name)
    mod.__file__ = dbfs_path
    sys.modules[module_name] = mod
    exec(compile(content, dbfs_path, "exec"), mod.__dict__)
    return mod

_src = "dbfs:/FileStore/cricket-pipeline/src"

# COMMAND ----------

from azure.storage.blob import BlobServiceClient

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
sport_id = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")
pq     = svc.get_container_client("process-queue")

QUIET_THRESHOLD_MINUTES = 60

# COMMAND ----------

dbutils.widgets.text("run_id",   "")
dbutils.widgets.text("event_id", "")

try:
    run_id = dbutils.widgets.get("run_id").strip()
except Exception:
    run_id = ""
if not run_id:
    run_id = "manual-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

try:
    event_id_filter = dbutils.widgets.get("event_id").strip()
except Exception:
    event_id_filter = ""

print(f"[read_pending_queue] run_id={run_id}  event_id_filter={event_id_filter or '(all)'}")

# COMMAND ----------

_load_from_dbfs("process_queue", f"{_src}/process_queue.py")
from process_queue import read_pending_markers, write_in_progress

def _scan_complete_events():
    complete = set()
    for blob in silver.list_blobs(name_starts_with="control/complete/"):
        fname = blob.name.rsplit("/", 1)[-1]
        if fname.startswith("event_id=") and fname.endswith(".json"):
            complete.add(fname[9:-5])
    return complete

def _scan_bronze_for_event(eid):
    prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={eid}/"
    result = defaultdict(list)
    for blob in bronze.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith("/manifest.json"):
            continue
        parts = blob.name.split("/")
        e = next((p[9:]  for p in parts if p.startswith("event_id=")), None)
        fi = next((p[3:]  for p in parts if p.startswith("fi=")),       None)
        sid = next((p[12:] for p in parts if p.startswith("snapshot_id=")), None)
        if e and sid:
            result[e].append((blob.name, blob.last_modified, fi or "", sid))
    return result, sum(len(v) for v in result.values())

# COMMAND ----------

script_start_utc = datetime.now(timezone.utc)
n_blobs_scanned  = 0

if event_id_filter:
    print(f"[read_pending_queue] Backfill — scanning bronze for event_id={event_id_filter}")
    events_dict, n_blobs_scanned = _scan_bronze_for_event(event_id_filter)
else:
    t0 = time.monotonic()
    print("[read_pending_queue] Reading pending markers...")
    events_dict = read_pending_markers(pq)
    print(f"[read_pending_queue] Marker read: {time.monotonic()-t0:.1f}s")

complete_events = _scan_complete_events()
print(f"[read_pending_queue] Silver complete events: {len(complete_events)}")

if event_id_filter and event_id_filter in complete_events:
    try:
        silver.get_blob_client(f"control/complete/event_id={event_id_filter}.json").delete_blob()
        complete_events.discard(event_id_filter)
        print(f"[read_pending_queue] Deleted complete marker for {event_id_filter}")
    except Exception:
        pass

# Load currently-live events from the ingestion control file (updated every 5s).
# Any event_id in this list is actively live — skip regardless of quiet timer.
live_eids = set()
if not event_id_filter:
    try:
        live_raw = json.loads(
            bronze.get_blob_client("betsapi/control/active_inplay_fi/latest.json")
                  .download_blob().readall()
        )
        live_eids = {
            str(m.get("event_id") or "")
            for m in (live_raw.get("active_matches") or [])
            if m.get("event_id")
        }
        print(f"[read_pending_queue] Currently live events: {len(live_eids)}")
    except Exception as e:
        print(f"[read_pending_queue] Could not load live control file (non-fatal): {e}")

now    = datetime.now(timezone.utc)
cutoff = now - timedelta(minutes=QUIET_THRESHOLD_MINUTES)

filtered_dict    = {}
skipped_complete = skipped_active = 0

for eid, manifests in events_dict.items():
    if eid in complete_events:
        skipped_complete += len(manifests)
        continue
    if not event_id_filter:
        # Skip if currently live (ingestion function confirms match in progress)
        if eid in live_eids:
            skipped_active += len(manifests)
            continue
        # Skip if last snapshot is less than 60 min old
        latest = max((cap for _, cap, _, _ in manifests if cap is not None), default=None)
        if latest and latest > cutoff:
            skipped_active += len(manifests)
            continue
    filtered_dict[eid] = manifests

total_events    = len(filtered_dict)
total_snapshots = sum(len(v) for v in filtered_dict.values())
print(f"[read_pending_queue] {total_events} events | {total_snapshots} snapshots to process")
print(f"[read_pending_queue]   skipped complete: {skipped_complete}")
print(f"[read_pending_queue]   skipped active  : {skipped_active}")

write_in_progress(pq, run_id, filtered_dict, n_blobs_scanned=n_blobs_scanned)

elapsed = (datetime.now(timezone.utc) - script_start_utc).total_seconds()
print(f"\n── Done ── {elapsed:.1f}s  |  {total_events} events queued  |  {total_snapshots} snapshots")

dbutils.notebook.exit(json.dumps({
    "events_queued":    total_events,
    "snapshots_queued": total_snapshots,
    "duration_seconds": round(elapsed, 1),
}))
