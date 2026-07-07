"""
One-time patchwork script: re-queue all existing gold events for reprocessing.

Run this ONCE after deploying the capture_final_scores pipeline activity.
It deletes the silver 'complete' marker for every event and writes a fresh
pending marker so that the next pl_build_ended_match run picks up all events,
runs capture_final_scores → bronze_to_silver → silver_to_gold for each, and
rebuilds the gold tracker with the correct final score from event_final.

Usage:
  python3 scripts/re_queue_all_events.py [--dry-run]

  --dry-run   Print what would happen without writing or deleting anything.

Prerequisites (same as repair_event_finals.py):
  export DATA_STORAGE_CONNECTION_STRING=$(az keyvault secret show \
      --vault-name kv-ramanuj --name DATA-STORAGE-CONNECTION-STRING \
      --query value -o tsv)
  python3 scripts/re_queue_all_events.py
"""

import json, os, sys, time
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient

CONN_STR = os.environ.get("DATA_STORAGE_CONNECTION_STRING", "")
DRY_RUN  = "--dry-run" in sys.argv

if not CONN_STR:
    print("ERROR: DATA_STORAGE_CONNECTION_STRING not set.")
    print("  export DATA_STORAGE_CONNECTION_STRING=$(az keyvault secret show \\")
    print("      --vault-name kv-ramanuj --name DATA-STORAGE-CONNECTION-STRING \\")
    print("      --query value -o tsv)")
    sys.exit(1)

svc    = BlobServiceClient.from_connection_string(CONN_STR)
gold   = svc.get_container_client("gold")
silver = svc.get_container_client("silver")
pq     = svc.get_container_client("process-queue")

# ---------------------------------------------------------------------------
# 1. Collect all event IDs from gold trackers
# ---------------------------------------------------------------------------

print("Scanning gold for event IDs...")
event_ids = []
for blob in gold.list_blobs(name_starts_with="event_id="):
    if not blob.name.endswith("/innings_tracker.json"):
        continue
    parts    = blob.name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    if eid_part:
        event_ids.append(eid_part[9:])

print(f"Found {len(event_ids)} events in gold\n")

# ---------------------------------------------------------------------------
# 2. For each event: delete silver complete marker + write pending marker
# ---------------------------------------------------------------------------

requeued = already_pending = skipped_delete = 0
now_iso  = datetime.now(timezone.utc).isoformat()

for eid in event_ids:
    complete_path = f"control/complete/event_id={eid}.json"
    pending_path  = f"pending/event_id={eid}.json"

    # Delete silver complete marker so read_pending_queue won't skip this event
    try:
        if not DRY_RUN:
            silver.get_blob_client(complete_path).delete_blob()
    except Exception:
        skipped_delete += 1  # didn't exist — that's fine

    # Write pending marker so read_pending_queue picks this event up
    pending_blob = {
        "event_id":       eid,
        "queued_at_utc":  now_iso,
        "source":         "re_queue_all_events",
    }
    try:
        if not DRY_RUN:
            pq.get_blob_client(pending_path).upload_blob(
                json.dumps(pending_blob, indent=2).encode(),
                overwrite=True,
            )
        requeued += 1
    except Exception as e:
        print(f"  [ERROR] {eid}: {e}")

    time.sleep(0.02)  # gentle rate limit

print(f"\n{'[DRY RUN] ' if DRY_RUN else ''}Done.")
print(f"  Events re-queued   : {requeued}")
print(f"  Complete markers not found (ok): {skipped_delete}")

if not DRY_RUN and requeued:
    print(f"""
Next steps:
  1. Trigger pl_build_ended_match in ADF (manual run).
  2. The pipeline will process all {requeued} events through:
       capture_final_scores → bronze_to_silver → silver_to_gold
     which will write the correct final scores into gold trackers.
  3. After the run, all ended match scores will be correct.
     You do NOT need to run this script again — future matches are
     handled automatically by capture_final_scores in the pipeline.
""")
