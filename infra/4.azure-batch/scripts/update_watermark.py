"""
Azure Batch script: update_watermark
Reads landing/{run_id}/index.json and advances the watermark to mark
this pipeline run as permanently committed.

Must be the LAST step in the success path. Skipped (exits 0) in backfill mode
(EVENT_ID set) because backfill runs must never advance the shared watermark.

Environment variables (from ADF Custom activity extendedProperties or activity.json):
  KEY_VAULT_URI              — e.g. https://kv-ramanuj.vault.azure.net/
  MANAGED_IDENTITY_CLIENT_ID — client ID of the pool managed identity
  RUN_ID                     — ADF pipeline run ID (@pipeline().RunId)
  EVENT_ID                   — if set, script exits without updating watermark

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
"""

import os, sys, json, time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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

svc     = BlobServiceClient.from_connection_string(conn_str)
landing = svc.get_container_client("landing")

run_id          = os.environ.get("RUN_ID", "unknown")
event_id_filter = os.environ.get("EVENT_ID", "").strip()

print(f"[update_watermark] run_id={run_id}  event_id_filter={event_id_filter or '(none)'}")

if event_id_filter:
    print("[update_watermark] Backfill mode — watermark NOT updated. Exiting.")
    sys.exit(0)

from landing_index import update_watermark_from_index

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

try:
    update_watermark_from_index(landing, run_id)
    status = "ok"
except Exception as e:
    print(f"[update_watermark] ERROR: {e}")
    status = "failed"
    raise
finally:
    elapsed = time.monotonic() - run_start
    script_finished_utc = datetime.now(timezone.utc)
    try:
        gold = svc.get_container_client("gold")
        log_date = script_start_utc.strftime("%Y%m%d")
        log_time = script_start_utc.strftime("%H%M%S")
        gold.get_blob_client(f"logs/pl_build_ended_match/{log_date}/{log_time}_update_watermark.json").upload_blob(
            json.dumps({
                "script":           "update_watermark",
                "run_id":           run_id,
                "run_date":         script_start_utc.strftime("%Y-%m-%d"),
                "started_at_utc":   script_start_utc.isoformat(),
                "finished_at_utc":  script_finished_utc.isoformat(),
                "duration_seconds": round(elapsed, 2),
                "status":           status,
            }, indent=2).encode(), overwrite=True
        )
    except Exception as log_ex:
        print(f"[log write failed — non-fatal]: {log_ex}")
