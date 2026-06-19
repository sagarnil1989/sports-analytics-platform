"""
Azure Batch script: index_new_snapshots
Scans bronze for new manifests since the last watermark and writes
a per-run landing index to landing/{run_id}/index.json.

Environment variables (from ADF Custom activity extendedProperties or activity.json):
  KEY_VAULT_URI              — e.g. https://kv-ramanuj.vault.azure.net/
  MANAGED_IDENTITY_CLIENT_ID — client ID of the pool managed identity
  RUN_ID                     — ADF pipeline run ID (@pipeline().RunId)
  EVENT_ID                   — optional; if set, only indexes that event

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
"""

import os, sys, json, time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ADF extendedProperties → os.environ fallback via activity.json
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

svc     = BlobServiceClient.from_connection_string(conn_str)
bronze  = svc.get_container_client("bronze")
landing = svc.get_container_client("landing")

run_id          = os.environ.get("RUN_ID", "unknown")
event_id_filter = os.environ.get("EVENT_ID", "").strip()

print(f"[index_new_snapshots] run_id={run_id}  event_id_filter={event_id_filter or '(all)'}")

from landing_index import scan_bronze_to_landing, set_watermark

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

blobs_scanned = None
new_entries   = None

try:
    scan_start_utc, new_entries, blobs_scanned = scan_bronze_to_landing(
        bronze, landing, sport_id, run_id, event_id_filter=event_id_filter or None
    )
    # Update watermark only if not in targeted backfill mode
    if not event_id_filter:
        set_watermark(landing, run_id, scan_start_utc, new_entries, blobs_scanned)
        print(f"[index_new_snapshots] Watermark updated to {scan_start_utc.isoformat()}")
    else:
        print(f"[index_new_snapshots] Backfill mode — watermark NOT updated")
    status = "ok"
except Exception as e:
    print(f"[index_new_snapshots] ERROR: {e}")
    status = "failed"
    raise
finally:
    elapsed = time.monotonic() - run_start
    script_finished_utc = datetime.now(timezone.utc)
    try:
        gold = svc.get_container_client("gold")
        log_date = script_start_utc.strftime("%Y%m%d")
        log_time = script_start_utc.strftime("%H%M%S")
        gold.get_blob_client(f"logs/pl_build_ended_match/{log_date}/{log_time}_index_new_snapshots.json").upload_blob(
            json.dumps({
                "script":           "index_new_snapshots",
                "run_id":           run_id,
                "run_date":         script_start_utc.strftime("%Y-%m-%d"),
                "started_at_utc":   script_start_utc.isoformat(),
                "finished_at_utc":  script_finished_utc.isoformat(),
                "duration_seconds": round(elapsed, 2),
                "status":           status,
                "event_id_filter":  event_id_filter or None,
                "blobs_scanned":    blobs_scanned if status == "ok" else None,
                "new_entries":      new_entries    if status == "ok" else None,
            }, indent=2).encode(), overwrite=True
        )
    except Exception as log_ex:
        print(f"[log write failed — non-fatal]: {log_ex}")
