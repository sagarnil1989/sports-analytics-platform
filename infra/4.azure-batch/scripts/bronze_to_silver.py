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

import os, sys, json, time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# ---------------------------------------------------------------------------
# Bootstrap secrets from Key Vault via managed identity
# ---------------------------------------------------------------------------

_kv_uri = os.environ["KEY_VAULT_URI"]
_cred   = ManagedIdentityCredential()
_kv     = SecretClient(vault_url=_kv_uri, credential=_cred)

conn_str = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
sport_id = _kv.get_secret("SPORT-ID").value

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")

QUIET_THRESHOLD_MINUTES = 60
PARALLEL_WORKERS        = 128

event_id_filter = os.environ.get("EVENT_ID", "").strip()
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
# Step 1+2: Scan bronze manifests AND silver markers in parallel
# ---------------------------------------------------------------------------

def _scan_bronze():
    result = defaultdict(list)
    for blob in bronze.list_blobs(name_starts_with=f"betsapi/inplay_snapshot/sport_id={sport_id}/"):
        if not blob.name.endswith("/manifest.json"):
            continue
        parts = blob.name.split("/")
        eid   = next((p[9:]  for p in parts if p.startswith("event_id=")),    None)
        fi    = next((p[3:]  for p in parts if p.startswith("fi=")),          None)
        sid   = next((p[12:] for p in parts if p.startswith("snapshot_id=")), None)
        if not eid or not sid:
            continue
        if event_id_filter and eid != event_id_filter:
            continue
        result[eid].append((blob.name, blob.last_modified, fi or "", sid))
    return result

def _scan_complete_events():
    complete = set()
    for blob in silver.list_blobs(name_starts_with="control/complete/"):
        fname = blob.name.rsplit("/", 1)[-1]
        if fname.startswith("event_id=") and fname.endswith(".json"):
            complete.add(fname[9:-5])
    return complete

t0 = time.monotonic()
print("Scanning bronze + silver in parallel...")
with ThreadPoolExecutor(max_workers=2) as pool:
    f_bronze   = pool.submit(_scan_bronze)
    f_complete = pool.submit(_scan_complete_events)
    bronze_events   = f_bronze.result()
    complete_events = f_complete.result()

total_events    = len(bronze_events)
total_manifests = sum(len(v) for v in bronze_events.values())
print(f"  bronze   : {total_events} events | {total_manifests} snapshots")
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
run_start = time.monotonic()
done = failed = 0
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
        except Exception as ex:
            failed += 1
            failed_eids.add(item[1])
            failed_items.append((item[1], item[2], str(ex)))

elapsed = time.monotonic() - run_start
print(f"\n── Done ──")
print(f"  processed : {done}")
print(f"  failed    : {failed}")
print(f"  total time: {elapsed/60:.1f} min  ({elapsed:.0f}s)")
if elapsed > 0:
    print(f"  avg rate  : {done/elapsed:.1f} snapshots/s")

if failed_items:
    print(f"\nFailed snapshots:")
    for eid, sid, err in failed_items[:20]:
        print(f"  event_id={eid}  snapshot_id={sid}  error={err}")

# ---------------------------------------------------------------------------
# Step 5: Write event-level complete markers
# ---------------------------------------------------------------------------

complete_written = 0
now_iso = datetime.now(timezone.utc).isoformat()

for eid, success_count in succeeded_by_event.items():
    if eid in failed_eids:
        continue
    if success_count != expected_by_event[eid]:
        continue
    silver.get_blob_client(f"control/complete/event_id={eid}.json").upload_blob(
        json.dumps({"completed_at_utc": now_iso, "snapshot_count": success_count}),
        overwrite=True,
    )
    complete_written += 1

print(f"\n  complete markers written : {complete_written}")
print(f"  events with failures (not marked complete) : {len(failed_eids)}")

if failed > 0:
    sys.exit(1)
