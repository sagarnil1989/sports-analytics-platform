# Databricks notebook: bronze_dedup_cleanup
# One-time (re-runnable) cleanup: removes duplicate bronze inplay snapshots for ended matches.
#
# A bug in _payload_hash() caused dedup to never fire — the full JSON wrapper (including
# called_at_utc) was hashed instead of just the response body. This notebook retroactively
# applies the correct dedup rule to all ended matches and deletes the redundant snapshots.
#
# ALWAYS run with DRY_RUN=true first to verify scope before deleting.
# Safe to re-run: already-deleted snapshots are simply not found.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob"], check=True)

# COMMAND ----------

dbutils.widgets.text("event_id", "", "Event ID (blank = all ended matches)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run (true = report only, no deletes)")
dbutils.widgets.text("max_workers", "32", "Parallel workers")

EVENT_ID   = dbutils.widgets.get("event_id").strip()
DRY_RUN    = dbutils.widgets.get("dry_run").strip().lower() == "true"
MAX_WORKERS = int(dbutils.widgets.get("max_workers").strip() or "32")

print(f"Event ID  : {EVENT_ID or '(all ended matches)'}")
print(f"Dry run   : {DRY_RUN}")
print(f"Workers   : {MAX_WORKERS}")

# COMMAND ----------

import hashlib
import json
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
svc      = BlobServiceClient.from_connection_string(conn_str)
bronze   = svc.get_container_client("bronze")

HEARTBEAT_SECONDS = 300  # mirror of SNAPSHOT_HEARTBEAT_SECONDS in bronze.py

def _dl(path):
    try:
        return json.loads(bronze.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

def _payload_hash(data):
    """Exact replica of the fixed _payload_hash() in bronze.py — hashes response body only."""
    body = data.get("response", {}).get("body") if isinstance(data, dict) else data
    return hashlib.md5(json.dumps(body, sort_keys=True).encode()).hexdigest()

# COMMAND ----------
# ── 1. Resolve which event IDs to process ─────────────────────────────────────

if EVENT_ID:
    event_ids = [EVENT_ID]
    print(f"Single event mode: {EVENT_ID}")
else:
    ended_index = _dl("cricket/ended/latest/index.json")
    if not ended_index:
        dbutils.notebook.exit("Could not load ended match index — run discover_cricket_ended first")
    event_ids = [str(m["event_id"]) for m in (ended_index.get("matches") or []) if m.get("event_id")]
    print(f"Loaded {len(event_ids)} ended matches from index")

# COMMAND ----------
# ── 2. For each event: list all snapshot manifest paths ───────────────────────

def list_snapshots_for_event(eid):
    """Returns list of (snapshot_id, fi, manifest_blob_path) sorted chronologically."""
    prefix = f"betsapi/inplay_snapshot/sport_id=3/event_id={eid}/"
    snaps = []
    for blob in bronze.list_blobs(name_starts_with=prefix):
        if not blob.name.endswith("/manifest.json"):
            continue
        parts = blob.name.split("/")
        sid = next((p.replace("snapshot_id=", "") for p in parts if p.startswith("snapshot_id=")), None)
        fi  = next((p.replace("fi=", "")          for p in parts if p.startswith("fi=")),          None)
        if sid:
            snaps.append({"snapshot_id": sid, "fi": fi, "manifest_path": blob.name})
    snaps.sort(key=lambda x: x["snapshot_id"])
    return snaps

print("Listing snapshots for all events (may take a minute)...")
event_snapshot_map = {}
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futs = {ex.submit(list_snapshots_for_event, eid): eid for eid in event_ids}
    for fut in as_completed(futs):
        eid = futs[fut]
        snaps = fut.result()
        if snaps:
            event_snapshot_map[eid] = snaps

total_snapshots = sum(len(v) for v in event_snapshot_map.values())
print(f"Events with snapshots : {len(event_snapshot_map)}")
print(f"Total snapshots       : {total_snapshots}")

# COMMAND ----------
# ── 3. Download stats + odds payloads and compute hashes ─────────────────────
# Only 2 files per snapshot downloaded — same two files the live dedup checks.

def _base_path(manifest_path):
    return manifest_path.rsplit("/manifest.json", 1)[0]

def _parse_ts(sid):
    """Parse snapshot_id like 20260523T140428Z into a UTC datetime."""
    try:
        return datetime.strptime(sid, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None

def load_hashes_for_snapshot(snap):
    base = _base_path(snap["manifest_path"])
    stats = _dl(f"{base}/api_live_market_stats.json")
    odds  = _dl(f"{base}/api_live_market_odds.json")
    return {
        **snap,
        "stats_hash": _payload_hash(stats) if stats else None,
        "odds_hash":  _payload_hash(odds)  if odds  else None,
        "snap_time":  _parse_ts(snap["snapshot_id"]),
    }

print("Loading hashes for all snapshots (parallel)...")
# Flatten all snapshots across all events into one list for bulk parallel fetch
all_snaps_flat = []
for eid, snaps in event_snapshot_map.items():
    for s in snaps:
        all_snaps_flat.append({**s, "event_id": eid})

hashed_snaps = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futs = {ex.submit(load_hashes_for_snapshot, s): s for s in all_snaps_flat}
    for fut in as_completed(futs):
        hashed_snaps.append(fut.result())

hashed_snaps.sort(key=lambda x: (x["event_id"], x["snapshot_id"]))
print(f"Hashes loaded for {len(hashed_snaps)} snapshots")

# COMMAND ----------
# ── 4. Apply dedup logic — mark which snapshots to delete ────────────────────
#
# Rule (mirrors the fixed bronze.py dedup):
#   DELETE a snapshot if:
#     - stats_hash == previous kept snapshot's stats_hash
#     - AND odds_hash == previous kept snapshot's odds_hash
#     - AND time since previous kept snapshot < HEARTBEAT_SECONDS (300s)
#   KEEP otherwise (content changed, or heartbeat interval exceeded, or hashes missing)

from collections import defaultdict

to_delete   = []   # list of snapshot dicts that should be removed
to_keep     = []   # snapshots that pass the dedup gate

event_stats = defaultdict(lambda: {"total": 0, "delete": 0})

# Group back by event_id (already sorted)
from itertools import groupby
from operator import itemgetter

for eid, group in groupby(hashed_snaps, key=itemgetter("event_id")):
    snaps_for_event = list(group)
    last_stats_hash = None
    last_odds_hash  = None
    last_kept_time  = None

    for snap in snaps_for_event:
        event_stats[eid]["total"] += 1
        sh = snap["stats_hash"]
        oh = snap["odds_hash"]
        t  = snap["snap_time"]

        seconds_since = (
            (t - last_kept_time).total_seconds()
            if (t and last_kept_time) else 9999
        )

        is_duplicate = (
            sh is not None
            and oh is not None
            and sh == last_stats_hash
            and oh == last_odds_hash
            and seconds_since < HEARTBEAT_SECONDS
        )

        if is_duplicate:
            to_delete.append(snap)
            event_stats[eid]["delete"] += 1
        else:
            to_keep.append(snap)
            last_stats_hash = sh
            last_odds_hash  = oh
            last_kept_time  = t

print(f"Snapshots to KEEP   : {len(to_keep)}")
print(f"Snapshots to DELETE : {len(to_delete)}")
if total_snapshots > 0:
    print(f"Reduction           : {len(to_delete)/total_snapshots*100:.1f}%")

# COMMAND ----------
# ── 5. Per-event summary table ────────────────────────────────────────────────

import pandas as pd

summary_rows = []
for eid, st in sorted(event_stats.items(), key=lambda x: -x[1]["delete"]):
    summary_rows.append({
        "event_id":    eid,
        "total":       str(st["total"]),
        "keep":        str(st["total"] - st["delete"]),
        "delete":      str(st["delete"]),
        "reduction_%": f"{st['delete']/st['total']*100:.0f}" if st["total"] else "0",
    })

summary_df = pd.DataFrame(summary_rows)
print(f"\nTop events by duplicates removed:")
display(spark.createDataFrame(summary_df.head(30)))

# COMMAND ----------
# ── 6. Delete — or report in dry-run mode ────────────────────────────────────

def delete_snapshot(snap):
    """Delete all blobs under a snapshot folder. Returns count of blobs deleted."""
    base = _base_path(snap["manifest_path"])
    prefix = base + "/"
    blobs = list(bronze.list_blobs(name_starts_with=prefix))
    for blob in blobs:
        try:
            bronze.delete_blob(blob.name)
        except ResourceNotFoundError:
            pass  # already gone
    return len(blobs)

if DRY_RUN:
    print(f"\n[DRY RUN] Would delete {len(to_delete)} snapshots across {len(event_stats)} events.")
    print("Set dry_run=false to perform the actual deletion.")
else:
    print(f"\nDeleting {len(to_delete)} duplicate snapshots...")
    deleted_blobs = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(delete_snapshot, s): s for s in to_delete}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                deleted_blobs += fut.result()
            except Exception as e:
                failed += 1
                print(f"  WARN: delete failed — {e}")
            if i % 500 == 0:
                print(f"  Progress: {i}/{len(to_delete)} snapshots processed")

    print(f"\nDone.")
    print(f"  Snapshots deleted : {len(to_delete) - failed}")
    print(f"  Blobs deleted     : {deleted_blobs}")
    print(f"  Failures          : {failed}")
    print(f"  Snapshots kept    : {len(to_keep)}")
