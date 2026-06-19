"""
Azure Batch script: bronze_to_silver
Reads raw bronze API snapshots and writes structured silver layer data.

Environment variables set by ADF Custom activity extendedProperties:
  KEY_VAULT_URI  — e.g. https://kv-ramanuj.vault.azure.net/
  EVENT_ID       — optional; if set, processes only that match (backfill mode)

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
"""

import os, sys, json, time, threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ADF Custom Activity writes extendedProperties to activity.json in the working
# directory. Inject them into os.environ so the rest of the script reads them
# the same way regardless of how ADF passes them.
_activity_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "activity.json")
if os.path.exists(_activity_json_path):
    with open(_activity_json_path) as _f:
        _ext = json.load(_f).get("typeProperties", {}).get("extendedProperties", {})
    for _k, _v in _ext.items():
        os.environ.setdefault(_k, str(_v))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# ---------------------------------------------------------------------------
# Bootstrap secrets from Key Vault via managed identity
# ---------------------------------------------------------------------------

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

conn_str = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
sport_id = _kv.get_secret("SPORT-ID").value

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")

QUIET_THRESHOLD_MINUTES = 60
PARALLEL_WORKERS        = 128

run_id          = os.environ.get("RUN_ID", "unknown")
event_id_filter = os.environ.get("EVENT_ID", "").strip()
print(f"run_id: {run_id}")
print(f"event_id filter: {event_id_filter or '(all quiet matches)'}")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Step 1+2: Read landing index AND silver markers in parallel
# ---------------------------------------------------------------------------

from landing_index import read_landing_index

landing = svc.get_container_client("landing")

def _read_landing():
    return read_landing_index(landing, run_id)

def _scan_complete_events():
    complete = set()
    for blob in silver.list_blobs(name_starts_with="control/complete/"):
        fname = blob.name.rsplit("/", 1)[-1]
        if fname.startswith("event_id=") and fname.endswith(".json"):
            complete.add(fname[9:-5])
    return complete

t0 = time.monotonic()
print("Reading landing index + silver markers in parallel...")
with ThreadPoolExecutor(max_workers=2) as pool:
    f_landing  = pool.submit(_read_landing)
    f_complete = pool.submit(_scan_complete_events)
    bronze_events   = f_landing.result()
    complete_events = f_complete.result()

total_events    = len(bronze_events)
total_manifests = sum(len(v) for v in bronze_events.values())
print(f"  landing  : {total_events} events | {total_manifests} snapshots (since last watermark)")
print(f"  complete : {len(complete_events)} events already done (will skip)")
print(f"  listing took {time.monotonic()-t0:.1f}s")

# ---------------------------------------------------------------------------
# Step 3: Build work list
# ---------------------------------------------------------------------------

t0 = time.monotonic()
print("\nBuilding work list...")

now    = datetime.now(timezone.utc)
cutoff = now - timedelta(minutes=QUIET_THRESHOLD_MINUTES)

if event_id_filter and event_id_filter in complete_events:
    try:
        silver.get_blob_client(f"control/complete/event_id={event_id_filter}.json").delete_blob()
        complete_events.discard(event_id_filter)
        print(f"  Deleted complete marker for {event_id_filter} — full reprocess")
    except Exception:
        pass

work_items = []
skipped_complete = skipped_active = 0

for eid, manifests in bronze_events.items():
    if eid in complete_events:
        skipped_complete += len(manifests)
        continue
    if not event_id_filter:
        latest_ts = max((lm for _, lm, _, _ in manifests if lm), default=None)
        if latest_ts and latest_ts > cutoff:
            skipped_active += len(manifests)
            continue
    for path, _, fi, sid in manifests:
        work_items.append((path, eid, sid, fi))

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
    sys.exit(0)

# ---------------------------------------------------------------------------
# Step 4: Process in parallel
# ---------------------------------------------------------------------------

from snapshot_parser import silver_parse_snapshot, silver_write_outputs

def process_snapshot(item):
    path, eid, sid, fi = item
    base = path.removesuffix("/manifest.json")

    manifest               = _dl_required(bronze, path)
    events_inplay_payload  = (_dl(bronze, f"{base}/api_inplay_event_list.json")
                               or _dl(bronze, f"{base}/events_inplay_full.json")
                               or _dl_required(bronze, f"{base}/events_inplay.json"))
    bet365_event_payload   = (_dl(bronze, f"{base}/api_live_market_odds.json")
                               or _dl(bronze, f"{base}/bet365_event_by_fi.json")
                               or _dl_required(bronze, f"{base}/bet365_event.json"))
    bet365_stats_payload   = _dl(bronze, f"{base}/api_live_market_stats.json")
    event_odds_payload     = (_dl(bronze, f"{base}/api_event_odds.json")
                               or _dl(bronze, f"{base}/event_odds_by_event_id.json")
                               or _dl_required(bronze, f"{base}/event_odds.json"))
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
run_start = time.monotonic()
done = failed = 0
failed_items = []
succeeded_by_event = defaultdict(int)
failed_eids        = set()
complete_written   = 0

# Per-event lock so only one thread writes the complete marker
_event_locks: dict[str, threading.Lock] = {eid: threading.Lock() for eid in expected_by_event}

def _maybe_write_complete_marker(eid: str, success_count: int) -> None:
    """Write the complete marker for an event if all its snapshots have succeeded."""
    global complete_written
    if success_count != expected_by_event[eid]:
        return
    now_iso = datetime.now(timezone.utc).isoformat()
    silver.get_blob_client(f"control/complete/event_id={eid}.json").upload_blob(
        json.dumps({"completed_at_utc": now_iso, "snapshot_count": success_count,
                    "run_id": run_id, "pipeline": "pl_build_ended_match"}),
        overwrite=True,
    )
    complete_written += 1
    print(f"  [marker] event_id={eid}  snapshots={success_count}")

with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
    futures = {pool.submit(process_snapshot, item): item for item in work_items}
    for future in as_completed(futures):
        item = futures[future]
        try:
            eid, sid = future.result()
            done += 1
            # Update per-event count under lock and immediately write marker if complete
            with _event_locks[eid]:
                succeeded_by_event[eid] += 1
                current_count = succeeded_by_event[eid]
                already_failed = eid in failed_eids
            if not already_failed:
                try:
                    _maybe_write_complete_marker(eid, current_count)
                except Exception as marker_ex:
                    print(f"  [marker ERROR] event_id={eid}: {marker_ex}")
            if done % 50 == 0 or done == len(work_items):
                elapsed = time.monotonic() - run_start
                rate    = done / elapsed if elapsed > 0 else 0
                eta_s   = (len(work_items) - done) / rate if rate > 0 else 0
                print(f"  {done}/{len(work_items)}  |  {rate:.1f}/s  |  ETA {eta_s/60:.1f} min  |  failed {failed}")
        except Exception as ex:
            failed += 1
            eid_failed = item[1]
            with _event_locks.get(eid_failed, threading.Lock()):
                failed_eids.add(eid_failed)
            failed_items.append((item[1], item[2], str(ex)))

elapsed = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)
print(f"\n── Done ──")
print(f"  processed : {done}")
print(f"  failed    : {failed}")
print(f"  total time: {elapsed/60:.1f} min  ({elapsed:.0f}s)")
if elapsed > 0:
    print(f"  avg rate  : {done/elapsed:.1f} snapshots/s")
print(f"\n  complete markers written : {complete_written}")
print(f"  events with failures (not marked complete) : {len(failed_eids)}")

if failed_items:
    print(f"\nFailed snapshots:")
    for eid, sid, err in failed_items[:20]:
        print(f"  event_id={eid}  snapshot_id={sid}  error={err}")

# ---------------------------------------------------------------------------
# Step 5: Write run log to gold
# ---------------------------------------------------------------------------

try:
    gold = svc.get_container_client("gold")
    log_date   = script_start_utc.strftime("%Y%m%d")
    log_time   = script_start_utc.strftime("%H%M%S")
    log_path   = f"logs/pl_build_ended_match/{log_date}/{log_time}_bronze_to_silver.json"
    run_log    = {
        "script":                   "bronze_to_silver",
        "run_date":                 script_start_utc.strftime("%Y-%m-%d"),
        "started_at_utc":           script_start_utc.isoformat(),
        "finished_at_utc":          script_finished_utc.isoformat(),
        "duration_seconds":         round(elapsed, 2),
        "status":                   "failed" if failed > 0 else "ok",
        "events_processed":         event_count,
        "snapshots_processed":      done,
        "snapshots_failed":         failed,
        "complete_markers_written": complete_written,
        "events_with_failures":     len(failed_eids),
        "skipped_complete":         skipped_complete,
        "skipped_active":           skipped_active,
    }
    gold.get_blob_client(log_path).upload_blob(
        json.dumps(run_log, indent=2).encode(), overwrite=True
    )
    print(f"\n  run log written: {log_path}")
except Exception as log_ex:
    print(f"\n  [log write failed — non-fatal]: {log_ex}")

if failed > 0:
    sys.exit(1)
