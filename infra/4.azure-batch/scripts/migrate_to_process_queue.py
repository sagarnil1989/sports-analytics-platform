"""
One-time migration script: migrate_to_process_queue

Scans all existing bronze manifests for sport_id=3 and writes pending markers
to process-queue/pending/ for any event that does NOT already have a silver
complete marker.  Run this ONCE after deploying the process-queue container
but before switching the ADF pipelines from index_new_snapshots to
read_pending_queue.

NOT an ADF activity — run manually:
  python3 migrate_to_process_queue.py

After this script finishes successfully, the normal ingestion function
(capture_inplay.py) will write new markers automatically going forward.

Environment:
  Reads activity.json if present (same bootstrap pattern as other batch scripts).
  Falls back to KEY_VAULT_URI + MANAGED_IDENTITY_CLIENT_ID from environment.

KV secrets:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
"""

import os, sys, json, time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Optional activity.json bootstrap (allows local runs with the same file)
_activity_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "activity.json")
if os.path.exists(_activity_json_path):
    with open(_activity_json_path) as _f:
        _ext = json.load(_f).get("typeProperties", {}).get("extendedProperties", {})
    for _k, _v in _ext.items():
        os.environ.setdefault(_k, str(_v))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

conn_str = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
sport_id = _kv.get_secret("SPORT-ID").value

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
silver = svc.get_container_client("silver")
pq     = svc.get_container_client("process-queue")

# ---------------------------------------------------------------------------
# Step 1: Collect silver complete markers — these events are already done
# ---------------------------------------------------------------------------

print("Step 1: Reading silver complete markers...")
t0 = time.monotonic()
complete_events = set()
for blob in silver.list_blobs(name_starts_with="control/complete/"):
    fname = blob.name.rsplit("/", 1)[-1]
    if fname.startswith("event_id=") and fname.endswith(".json"):
        complete_events.add(fname[9:-5])
print(f"  {len(complete_events)} events already complete in silver ({time.monotonic()-t0:.1f}s)")

# ---------------------------------------------------------------------------
# Step 2: Scan bronze for all manifests, grouped by event_id
# ---------------------------------------------------------------------------

print(f"\nStep 2: Scanning bronze for sport_id={sport_id} manifests...")
t0 = time.monotonic()

prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/"
events_to_migrate = {}  # {eid: [(manifest_path, fi, sid, captured_at_str), ...]}
blobs_scanned = 0

for blob in bronze.list_blobs(name_starts_with=prefix):
    if not blob.name.endswith("/manifest.json"):
        continue

    parts = blob.name.split("/")
    eid = next((p[9:]  for p in parts if p.startswith("event_id=")),    None)
    fi  = next((p[3:]  for p in parts if p.startswith("fi=")),          None)
    sid = next((p[12:] for p in parts if p.startswith("snapshot_id=")), None)

    if not eid or not sid:
        continue

    blobs_scanned += 1

    if eid in complete_events:
        # Already processed — skip; no pending marker needed
        continue

    if eid not in events_to_migrate:
        events_to_migrate[eid] = []

    # Use blob last_modified as a best-effort captured_at; None is also fine.
    captured_at = blob.last_modified.isoformat() if blob.last_modified else None
    events_to_migrate[eid].append((blob.name, fi or "", sid, captured_at))

elapsed = time.monotonic() - t0
print(f"  {blobs_scanned} manifests scanned in {elapsed:.1f}s")
print(f"  {len(events_to_migrate)} events need pending markers "
      f"(skipped {len(complete_events)} already-complete events)")

# ---------------------------------------------------------------------------
# Step 3: Write pending markers for each incomplete event
# ---------------------------------------------------------------------------

print(f"\nStep 3: Writing pending markers to process-queue/pending/...")
t0 = time.monotonic()

markers_written = 0
markers_skipped = 0  # already exist
errors          = 0

now_iso = datetime.now(timezone.utc).isoformat()

for eid, snapshots in events_to_migrate.items():
    for manifest_path, fi, sid, captured_at in snapshots:
        marker_path = f"pending/event_id={eid}/{sid}.json"

        # Check if marker already exists (idempotent re-run support)
        try:
            pq.get_blob_client(marker_path).get_blob_properties()
            markers_skipped += 1
            continue
        except Exception:
            pass  # Does not exist — write it

        marker = {
            "event_id":        eid,
            "snapshot_id":     sid,
            "fi":              fi,
            "manifest_path":   manifest_path,
            "captured_at_utc": captured_at,  # May be None — handled gracefully by read_pending_markers
            "migrated":        True,
            "migrated_at_utc": now_iso,
        }

        try:
            pq.get_blob_client(marker_path).upload_blob(
                json.dumps(marker, indent=2).encode(),
                overwrite=False,
            )
            markers_written += 1
        except Exception as ex:
            errors += 1
            print(f"  ERROR writing {marker_path}: {ex}")

        if (markers_written + markers_skipped) % 500 == 0:
            elapsed_so_far = time.monotonic() - t0
            rate = (markers_written + markers_skipped) / elapsed_so_far if elapsed_so_far > 0 else 0
            print(f"  written={markers_written}  skipped={markers_skipped}  errors={errors}  "
                  f"rate={rate:.0f}/s")

elapsed = time.monotonic() - t0
print(f"\n── Migration complete ──")
print(f"  blobs scanned         : {blobs_scanned}")
print(f"  events complete (skip): {len(complete_events)}")
print(f"  events migrated       : {len(events_to_migrate)}")
print(f"  markers written       : {markers_written}")
print(f"  markers skipped       : {markers_skipped}  (already existed — idempotent)")
print(f"  errors                : {errors}")
print(f"  time                  : {elapsed:.1f}s")

if errors > 0:
    print(f"\nWARNING: {errors} markers failed to write. Re-run the script — it is idempotent.")
    sys.exit(1)

print("\nDone. Run the ADF pipeline to verify read_pending_queue picks up the migrated markers.")
