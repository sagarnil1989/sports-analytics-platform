"""
Azure Batch script: delete_processed_markers
Replaces update_watermark.  After a successful pipeline run, deletes all
pending markers for the events that were processed in this run and writes
a last-successful-run sentinel.

Skipped in backfill mode (EVENT_ID set) because:
  - Backfill processes events that may not have pending markers (pre-migration
    bronze events were never written to process-queue/pending/)
  - The silver complete marker written by bronze_to_silver already prevents
    re-processing on subsequent normal runs

Environment variables (from ADF Custom activity extendedProperties or activity.json):
  KEY_VAULT_URI              — e.g. https://kv-ramanuj.vault.azure.net/
  MANAGED_IDENTITY_CLIENT_ID — client ID of the pool managed identity
  RUN_ID                     — ADF pipeline run ID (@pipeline().RunId)
  EVENT_ID                   — if set, script exits without deleting markers

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
"""

import os, sys, json, time
from datetime import datetime, timezone

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

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

conn_str = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value

svc = BlobServiceClient.from_connection_string(conn_str)
pq  = svc.get_container_client("process-queue")

run_id          = os.environ.get("RUN_ID", "unknown")
event_id_filter = os.environ.get("EVENT_ID", "").strip()

print(f"[delete_processed_markers] run_id={run_id}  event_id_filter={event_id_filter or '(none)'}")

if event_id_filter:
    print("[delete_processed_markers] Backfill mode — marker cleanup skipped. Exiting.")
    sys.exit(0)

from process_queue import read_in_progress, delete_pending_for_events, write_last_successful_run

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()
status           = "ok"
n_deleted        = 0
n_processed      = 0

try:
    # ── Step 1: Read in-progress file to know which events ran this cycle ────

    events_dict = read_in_progress(pq, run_id)
    all_event_ids = list(events_dict.keys())
    n_processed   = sum(len(v) for v in events_dict.values())

    print(f"[delete_processed_markers] {len(all_event_ids)} events processed this run "
          f"({n_processed} snapshots)")

    # ── Step 2: Delete pending markers for all events in this run ────────────

    n_deleted = delete_pending_for_events(pq, all_event_ids)

    # ── Step 3: Write last-successful-run sentinel ────────────────────────────

    write_last_successful_run(pq, run_id, n_processed)

except Exception as e:
    print(f"[delete_processed_markers] ERROR: {e}")
    status = "failed"
    raise

finally:
    elapsed             = time.monotonic() - run_start
    script_finished_utc = datetime.now(timezone.utc)
    try:
        gold     = svc.get_container_client("gold")
        log_date = script_start_utc.strftime("%Y%m%d")
        log_time = script_start_utc.strftime("%H%M%S")
        gold.get_blob_client(
            f"logs/pl_build_ended_match/{log_date}/{log_time}_delete_processed_markers.json"
        ).upload_blob(
            json.dumps({
                "script":             "delete_processed_markers",
                "run_id":             run_id,
                "run_date":           script_start_utc.strftime("%Y-%m-%d"),
                "started_at_utc":     script_start_utc.isoformat(),
                "finished_at_utc":    script_finished_utc.isoformat(),
                "duration_seconds":   round(elapsed, 2),
                "status":             status,
                "events_processed":   len(all_event_ids) if status == "ok" else None,
                "snapshots_processed": n_processed       if status == "ok" else None,
                "markers_deleted":    n_deleted           if status == "ok" else None,
            }, indent=2).encode(), overwrite=True
        )
    except Exception as log_ex:
        print(f"[log write failed — non-fatal]: {log_ex}")
