# Databricks notebook: bronze_to_silver
# Reads raw bronze API snapshots and writes structured silver layer data.
# Used by two ADF pipelines:
#   pl_build_ended_match (daily 02:00 CET) — no event_id, processes all matches quiet >=60 min
#   pl_backfill (manual)                   — pass event_id for one match, or empty for all quiet matches
# All orchestration is inline. Processes snapshots in parallel (128 threads) for fast blob I/O.

# COMMAND ----------

import subprocess
subprocess.run(["pip", "install", "--quiet", "azure-storage-blob", "azure-storage-file-datalake"], check=True)

# COMMAND ----------

import sys, os, json, time, types
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from azure.storage.blob import BlobServiceClient

sys.path.insert(0, "/dbfs/FileStore/cricket-pipeline/src/")

def _load_from_dbfs(module_name, dbfs_path):
    """Read module source from DBFS via REST API (no FUSE mount, works on Shared clusters)."""
    content = dbutils.fs.head(dbfs_path, 500000)
    mod = types.ModuleType(module_name)
    mod.__file__ = dbfs_path
    sys.modules[module_name] = mod
    exec(compile(content, dbfs_path, "exec"), mod.__dict__)
    return mod

_src = "dbfs:/FileStore/cricket-pipeline/src"

conn_str = dbutils.secrets.get("cricket-pipeline", "DATA_STORAGE_CONNECTION_STRING")
sport_id = dbutils.secrets.get("cricket-pipeline", "SPORT_ID")

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")
pq     = svc.get_container_client("process-queue")

QUIET_THRESHOLD_MINUTES = 60
PARALLEL_WORKERS        = 128  # IO-bound blob ops — threads >> cores is correct for network-bound work

def _dl(container, path):
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None

def _dl_required(container, path):
    data = _dl(container, path)
    if data is None:
        raise FileNotFoundError(f"Required blob not found: {path}")
    return data

class _IncompleteCapture(Exception):
    pass

# COMMAND ----------
# ── STEP 0: Read parameters ───────────────────────────────────────────────────

dbutils.widgets.text("event_id", "")
dbutils.widgets.text("run_id",   "")

try:
    event_id_filter = dbutils.widgets.get("event_id").strip()
except Exception:
    event_id_filter = ""

try:
    run_id = dbutils.widgets.get("run_id").strip()
except Exception:
    run_id = ""

if not run_id:
    run_id = "manual-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

print(f"run_id: {run_id}")
print(f"event_id filter: {event_id_filter or '(all quiet matches)'}")

# COMMAND ----------
# ── STEP 1 + 2: Read process-queue in-progress file AND silver markers in parallel ──

from concurrent.futures import ThreadPoolExecutor as _ScanPool

_load_from_dbfs("process_queue", f"{_src}/process_queue.py")
from process_queue import read_in_progress

def _read_in_progress():
    return read_in_progress(pq, run_id)

def _scan_complete_events():
    complete = set()
    for blob in silver.list_blobs(name_starts_with="control/complete/"):
        fname = blob.name.rsplit("/", 1)[-1]
        if fname.startswith("event_id=") and fname.endswith(".json"):
            complete.add(fname[9:-5])
    return complete

t0 = time.monotonic()
print("Reading process-queue in-progress + silver markers in parallel...")
with _ScanPool(max_workers=2) as pool:
    f_in_progress = pool.submit(_read_in_progress)
    f_complete    = pool.submit(_scan_complete_events)
    bronze_events   = f_in_progress.result()
    complete_events = f_complete.result()

total_events    = len(bronze_events)
total_manifests = sum(len(v) for v in bronze_events.values())
print(f"  in-progress : {total_events} events | {total_manifests} snapshots (from process-queue)")
print(f"  complete    : {len(complete_events)} events already done (will skip)")
print(f"  listing took {time.monotonic()-t0:.1f}s")

# COMMAND ----------
# ── STEP 3: Build work list ──────────────────────────────────────────────────

t0 = time.monotonic()
print("\nBuilding work list...")

now    = datetime.now(timezone.utc)
cutoff = now - timedelta(minutes=QUIET_THRESHOLD_MINUTES)

# Force-rebuild: if a specific event_id was passed, delete its complete marker so all
# its snapshots are treated as unprocessed and rerun from scratch.
if event_id_filter and event_id_filter in complete_events:
    try:
        silver.get_blob_client(f"control/complete/event_id={event_id_filter}.json").delete_blob()
        complete_events.discard(event_id_filter)
        print(f"  Deleted complete marker for {event_id_filter} — full reprocess")
    except Exception:
        pass

work_items = []   # (manifest_path, eid, sid, fi)
skipped_complete = skipped_active = 0

for eid, manifests in bronze_events.items():
    # Skip events already fully processed — one flag covers all their snapshots.
    if eid in complete_events:
        skipped_complete += len(manifests)
        continue

    # Without a specific event_id filter, skip matches that are still active.
    if not event_id_filter:
        latest_ts = max((lm for _, lm, _, _ in manifests if lm), default=None)
        if latest_ts and latest_ts > cutoff:
            skipped_active += len(manifests)
            continue

    for path, _, fi, sid in manifests:
        work_items.append((path, eid, sid, fi))

# Oldest snapshot first so backlog drains from the earliest match forward.
work_items.sort(key=lambda x: x[2])

expected_by_event = defaultdict(int)
for _, eid, _, _ in work_items:
    expected_by_event[eid] += 1

event_count = len(expected_by_event)
print(f"  {len(work_items)} snapshots to process across {event_count} events")
print(f"  {skipped_complete} snapshots in {len(complete_events)} complete events (skipped)")
print(f"  {skipped_active} in active matches (skipped, quieted less than {QUIET_THRESHOLD_MINUTES} min ago)")
print(f"  ({time.monotonic()-t0:.1f}s)")

if not work_items:
    print("\nNothing to process.")
    dbutils.notebook.exit("done: nothing to process")

# COMMAND ----------
# ── STEP 4: Process in parallel ─────────────────────────────────────────────

from snapshot_parser import silver_parse_snapshot, silver_write_outputs

def process_snapshot(item):
    """Download bronze files, parse to silver, write outputs + marker."""
    path, eid, sid, fi = item
    base = path.removesuffix("/manifest.json")

    manifest               = _dl_required(bronze, path)
    events_inplay_payload  = (_dl(bronze, f"{base}/api_inplay_event_list.json")
                               or _dl(bronze, f"{base}/events_inplay_full.json")
                               or _dl(bronze, f"{base}/events_inplay.json"))
    bet365_event_payload   = (_dl(bronze, f"{base}/api_live_market_odds.json")
                               or _dl(bronze, f"{base}/bet365_event_by_fi.json")
                               or _dl(bronze, f"{base}/bet365_event.json"))
    bet365_stats_payload   = _dl(bronze, f"{base}/api_live_market_stats.json")
    event_odds_payload     = (_dl(bronze, f"{base}/api_event_odds.json")
                               or _dl(bronze, f"{base}/event_odds_by_event_id.json")
                               or _dl(bronze, f"{base}/event_odds.json"))

    # Skip snapshots where core match-state payloads are missing — either all three
    # absent (ingestion bug, early June) or just events_inplay absent (network timeout).
    # event_odds is supplementary odds data; None is handled gracefully by the parser.
    if events_inplay_payload is None and bet365_event_payload is None and event_odds_payload is None:
        raise _IncompleteCapture(f"all core payloads missing — incomplete bronze capture")
    if events_inplay_payload is None:
        raise _IncompleteCapture(f"events_inplay missing — partial capture, no recoverable match state: {base}")
    if bet365_event_payload is None:
        raise FileNotFoundError(f"bet365_event missing: {base}")
    event_view_payload     = (_dl(bronze, f"{base}/api_event_view.json")
                               or _dl(bronze, f"{base}/event_view_by_event_id.json"))
    event_odds_summary     = (_dl(bronze, f"{base}/api_event_odds_summary.json")
                               or _dl(bronze, f"{base}/event_odds_summary_by_event_id.json"))
    lineage_payload        = _dl(bronze, f"{base}/lineage.json")

    parsed = silver_parse_snapshot(
        manifest,
        events_inplay_payload,
        bet365_event_payload,
        event_odds_payload,
        event_view_payload=event_view_payload,
        event_odds_summary_payload=event_odds_summary,
        lineage_payload=lineage_payload,
        bet365_event_stats_payload=bet365_stats_payload,
    )
    silver_write_outputs(silver, parsed, write_marker=False)
    return eid, sid


print(f"\nProcessing {len(work_items)} snapshots with {PARALLEL_WORKERS} parallel workers...")
script_start_utc = datetime.now(timezone.utc)
run_start  = time.monotonic()
done = failed = skipped_incomplete = 0
failed_items = []
succeeded_by_event = defaultdict(int)
failed_eids        = set()

with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
    futures = {pool.submit(process_snapshot, item): item for item in work_items}
    for future in as_completed(futures):
        item = futures[future]
        try:
            eid, sid = future.result()
            done += 1
            succeeded_by_event[eid] += 1
            if done % 50 == 0 or done == len(work_items):
                elapsed = time.monotonic() - run_start
                rate    = done / elapsed if elapsed > 0 else 0
                eta_s   = (len(work_items) - done) / rate if rate > 0 else 0
                print(f"  {done}/{len(work_items)}  |  {rate:.1f}/s  |  ETA {eta_s/60:.1f} min  |  failed {failed}")
        except _IncompleteCapture:
            skipped_incomplete += 1
            # Count as "processed" so event can still receive a complete marker
            succeeded_by_event[item[1]] += 1
        except Exception as ex:
            failed += 1
            failed_eids.add(item[1])
            failed_items.append((item[1], item[2], str(ex)))

elapsed = time.monotonic() - run_start
print(f"\n── Done ──")
print(f"  processed           : {done}")
print(f"  skipped (incomplete): {skipped_incomplete}")
print(f"  failed              : {failed}")
print(f"  total time: {elapsed/60:.1f} min  ({elapsed:.0f}s)")
print(f"  avg rate  : {done/elapsed:.1f} snapshots/s" if elapsed > 0 else "")

if failed_items:
    print(f"\nFailed snapshots:")
    for eid, sid, err in failed_items[:20]:
        print(f"  event_id={eid}  snapshot_id={sid}  error={err}")

# ── STEP 5: Write event-level complete markers ───────────────────────────────
# Only written when every snapshot for an event succeeded in this run.
# Next run will skip these events entirely — O(events) listing instead of O(snapshots).
# To force a full reprocess: run pl_backfill with event_id — Step 3 deletes the marker.

complete_written = 0
now_iso = datetime.now(timezone.utc).isoformat()

for eid, success_count in succeeded_by_event.items():
    if eid in failed_eids:
        continue
    if success_count != expected_by_event[eid]:
        continue
    silver.get_blob_client(f"control/complete/event_id={eid}.json").upload_blob(
        json.dumps({"completed_at_utc": now_iso, "snapshot_count": success_count,
                    "run_id": run_id, "pipeline": "pl_build_ended_match_databricks"}),
        overwrite=True,
    )
    complete_written += 1

print(f"\n  complete markers written : {complete_written}")
print(f"  events with failures (not marked complete) : {len(failed_eids)}")

# ── STEP 6: Write run log to gold ────────────────────────────────────────────
try:
    gold = svc.get_container_client("gold")
    script_finished_utc = datetime.now(timezone.utc)
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    log_path = f"logs/pl_build_ended_match/{log_date}/{log_time}_bronze_to_silver.json"
    gold.get_blob_client(log_path).upload_blob(
        json.dumps({
            "script":                   "bronze_to_silver",
            "run_date":                 script_start_utc.strftime("%Y-%m-%d"),
            "started_at_utc":           script_start_utc.isoformat(),
            "finished_at_utc":          script_finished_utc.isoformat(),
            "duration_seconds":         round(elapsed, 2),
            "status":                   "failed" if failed > 0 else "ok",
            "events_processed":         event_count,
            "snapshots_processed":          done,
            "snapshots_skipped_incomplete": skipped_incomplete,
            "snapshots_failed":             failed,
            "complete_markers_written":     complete_written,
            "events_with_failures":         len(failed_eids),
            "skipped_complete":             skipped_complete,
            "skipped_active":               skipped_active,
            "failed_snapshots":         [
                {"event_id": eid, "snapshot_id": sid, "error": err}
                for eid, sid, err in failed_items
            ],
        }, indent=2).encode(),
        overwrite=True,
    )
    print(f"\n  run log written: {log_path}")
except Exception as log_ex:
    print(f"\n  [log write failed — non-fatal]: {log_ex}")

if failed > 0:
    raise Exception(f"bronze_to_silver: {failed} snapshots failed — see run log for details")
