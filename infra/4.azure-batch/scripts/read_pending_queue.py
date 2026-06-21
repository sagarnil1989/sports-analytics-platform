"""
Azure Batch script: read_pending_queue
Replaces index_new_snapshots.  Instead of scanning 300k+ bronze blobs,
this script reads lightweight pending markers written by the ingestion
function at capture time.

Normal mode  (EVENT_ID not set):
  - Lists process-queue/pending/ markers (seconds, not 800 s)
  - Skips events already complete in silver (control/complete/*)
  - Skips events still active (latest captured_at < 60 min ago)
  - Writes process-queue/in-progress/{run_id}.json
  - Writes gold run log

Backfill mode (EVENT_ID set):
  - Scans bronze directly for that specific event_id (same as old
    index_new_snapshots backfill logic, no watermark check)
  - No active-match cutoff — caller wants to force-process
  - Writes process-queue/in-progress/{run_id}.json
  - Writes gold run log

Environment variables (from ADF Custom activity extendedProperties or activity.json):
  KEY_VAULT_URI              — e.g. https://kv-ramanuj.vault.azure.net/
  MANAGED_IDENTITY_CLIENT_ID — client ID of the pool managed identity
  RUN_ID                     — ADF pipeline run ID (@pipeline().RunId)
  EVENT_ID                   — optional; if set, backfill mode for that event

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
"""

import os, sys, json, time
from datetime import datetime, timezone, timedelta
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
pq     = svc.get_container_client("process-queue")

run_id          = os.environ.get("RUN_ID", "unknown")
event_id_filter = os.environ.get("EVENT_ID", "").strip()

print(f"[read_pending_queue] run_id={run_id}  event_id_filter={event_id_filter or '(all)'}")

from process_queue import read_pending_markers, write_in_progress

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

QUIET_THRESHOLD_MINUTES = 60

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scan_complete_events():
    """Return the set of event_ids that already have a silver complete marker."""
    complete = set()
    for blob in silver.list_blobs(name_starts_with="control/complete/"):
        fname = blob.name.rsplit("/", 1)[-1]
        if fname.startswith("event_id=") and fname.endswith(".json"):
            complete.add(fname[9:-5])
    return complete


def _scan_bronze_for_event(event_id: str) -> dict:
    """
    Backfill mode: scan bronze for all manifests belonging to event_id.
    Returns {event_id: [(manifest_path, None, fi, sid), ...]}

    Mirrors the logic in landing_index.scan_bronze_to_landing but scoped to
    a single event and without watermark filtering.
    """
    prefix  = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/"
    result  = defaultdict(list)
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
        # captured_at_utc is None for backfill bronze scans — use blob last_modified
        # if available as a best-effort timestamp.
        captured_at = blob.last_modified  # timezone-aware datetime from SDK
        result[eid].append((blob.name, captured_at, fi or "", sid))

    print(f"[read_pending_queue] backfill scan: {blobs_scanned} manifests for event_id={event_id}")
    return result, blobs_scanned

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()
n_blobs_scanned  = 0
status           = "ok"

try:
    # ── Step 1: Build events_dict ─────────────────────────────────────────────

    if event_id_filter:
        # Backfill mode — scan bronze directly, bypass pending markers
        print(f"[read_pending_queue] Backfill mode — scanning bronze for event_id={event_id_filter}")
        events_dict, n_blobs_scanned = _scan_bronze_for_event(event_id_filter)
    else:
        # Normal mode — read lightweight pending markers
        t0 = time.monotonic()
        print("[read_pending_queue] Reading pending markers from process-queue...")
        events_dict = read_pending_markers(pq)
        print(f"[read_pending_queue] Marker read took {time.monotonic()-t0:.1f}s")

    # ── Step 2: Skip events already complete in silver ────────────────────────

    t0 = time.monotonic()
    complete_events = _scan_complete_events()
    print(f"[read_pending_queue] Silver complete scan: {len(complete_events)} events done "
          f"({time.monotonic()-t0:.1f}s)")

    # In backfill mode, force-reprocess by removing the complete marker
    if event_id_filter and event_id_filter in complete_events:
        try:
            silver.get_blob_client(f"control/complete/event_id={event_id_filter}.json").delete_blob()
            complete_events.discard(event_id_filter)
            print(f"[read_pending_queue] Deleted complete marker for {event_id_filter} — full reprocess")
        except Exception:
            pass

    # ── Step 3: Load currently-live events from the ingestion control file ────
    # The ingestion function writes this every 5 seconds. Any event_id in this
    # list is actively live — do not process it regardless of the quiet timer.

    live_control = None
    live_eids    = set()
    if not event_id_filter:
        try:
            live_control = json.loads(
                bronze.get_blob_client("betsapi/control/active_inplay_fi/latest.json")
                      .download_blob().readall()
            )
            live_eids = {
                str(m.get("event_id") or "")
                for m in (live_control.get("active_matches") or [])
                if m.get("event_id")
            }
            print(f"[read_pending_queue] Currently live events: {len(live_eids)}")
        except Exception as e:
            print(f"[read_pending_queue] Could not load live control file (non-fatal): {e}")

    # ── Step 4: Filter — skip complete, live, and recently-active events ──────

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=QUIET_THRESHOLD_MINUTES)

    filtered_dict    = {}
    skipped_complete = 0
    skipped_active   = 0

    for eid, manifests in events_dict.items():
        if eid in complete_events:
            skipped_complete += len(manifests)
            continue

        # In backfill mode, never skip due to active-match checks
        if not event_id_filter:
            # Skip if the ingestion function says this match is currently live
            if eid in live_eids:
                skipped_active += len(manifests)
                continue

            # Skip if a snapshot was captured within the last 60 min — match
            # hasn't been quiet long enough to be considered finished
            latest_captured = max(
                (cap for _, cap, _, _ in manifests if cap is not None),
                default=None
            )
            if latest_captured and latest_captured > cutoff:
                skipped_active += len(manifests)
                continue

        filtered_dict[eid] = manifests

    total_events    = len(filtered_dict)
    total_snapshots = sum(len(v) for v in filtered_dict.values())
    print(f"[read_pending_queue] After filter: {total_events} events | {total_snapshots} snapshots")
    print(f"[read_pending_queue]   skipped complete : {skipped_complete} snapshots")
    print(f"[read_pending_queue]   skipped active   : {skipped_active} snapshots")

    # ── Step 4: Write in-progress file ────────────────────────────────────────

    write_in_progress(pq, run_id, filtered_dict, n_blobs_scanned=n_blobs_scanned)

    if not filtered_dict:
        print("[read_pending_queue] No events to process — wrote empty in-progress. Exiting.")

except Exception as e:
    print(f"[read_pending_queue] ERROR: {e}")
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
            f"logs/pl_build_ended_match/{log_date}/{log_time}_read_pending_queue.json"
        ).upload_blob(
            json.dumps({
                "script":           "read_pending_queue",
                "run_id":           run_id,
                "run_date":         script_start_utc.strftime("%Y-%m-%d"),
                "started_at_utc":   script_start_utc.isoformat(),
                "finished_at_utc":  script_finished_utc.isoformat(),
                "duration_seconds": round(elapsed, 2),
                "status":           status,
                "event_id_filter":  event_id_filter or None,
                "blobs_scanned":    n_blobs_scanned if event_id_filter else None,
                "events_queued":    len(filtered_dict) if status == "ok" else None,
                "snapshots_queued": sum(len(v) for v in filtered_dict.values()) if status == "ok" else None,
            }, indent=2).encode(), overwrite=True
        )
    except Exception as log_ex:
        print(f"[log write failed — non-fatal]: {log_ex}")
