"""
Azure Batch script: silver_to_gold
Reads silver innings data and rebuilds gold innings tracker files.

Environment variables set by ADF Custom activity extendedProperties:
  KEY_VAULT_URI  — e.g. https://kv-ramanuj.vault.azure.net/
  EVENT_ID       — optional; if set, rebuilds only that match (backfill mode)

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
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
os.environ["DATA_STORAGE_CONNECTION_STRING"] = conn_str
os.environ["SPORT_ID"]                       = _kv.get_secret("SPORT-ID").value

svc  = BlobServiceClient.from_connection_string(conn_str)
gold = svc.get_container_client("gold")

event_id = os.environ.get("EVENT_ID", "").strip() or None
print(f"[silver_to_gold] event_id={'ALL stale matches' if not event_id else event_id}")

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()
status           = "ok"

from gold_rebuild import gold_rebuild_ended_matches
try:
    gold_rebuild_ended_matches(event_id=event_id)
except Exception:
    status = "failed"
    raise
finally:
    elapsed             = time.monotonic() - run_start
    script_finished_utc = datetime.now(timezone.utc)
    try:
        log_date = script_start_utc.strftime("%Y%m%d")
        log_time = script_start_utc.strftime("%H%M%S")
        log_path = f"logs/pl_build_ended_match/{log_date}/{log_time}_silver_to_gold.json"
        run_log  = {
            "script":           "silver_to_gold",
            "run_date":         script_start_utc.strftime("%Y-%m-%d"),
            "started_at_utc":   script_start_utc.isoformat(),
            "finished_at_utc":  script_finished_utc.isoformat(),
            "duration_seconds": round(elapsed, 2),
            "status":           status,
            "event_id_filter":  event_id or "all",
        }
        gold.get_blob_client(log_path).upload_blob(
            json.dumps(run_log, indent=2).encode(), overwrite=True
        )
        print(f"\n  run log written: {log_path}")
    except Exception as log_ex:
        print(f"\n  [log write failed — non-fatal]: {log_ex}")
