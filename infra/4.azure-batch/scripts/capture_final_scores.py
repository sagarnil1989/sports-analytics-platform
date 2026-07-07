"""
Azure Batch script: capture_final_scores
Runs after read_pending_queue and before bronze_to_silver.

Reads the in-progress file (written by read_pending_queue) to get the exact
set of event IDs being processed this run, then calls /v1/event/view for each
and writes the result into the bronze event_final blob.

By the time bronze_to_silver and silver_to_gold run, the event_final blob is
already populated.  gold_rebuild.py reads it as the authoritative final score.

Environment variables (set by ADF extendedProperties):
  KEY_VAULT_URI               — e.g. https://kv-ramanuj.vault.azure.net/
  MANAGED_IDENTITY_CLIENT_ID  — user-assigned identity for the Batch pool
  RUN_ID                      — ADF pipeline run ID (@pipeline().RunId)
"""

import os, sys, json, time
import requests as _requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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

conn_str      = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
betsapi_token = _kv.get_secret("BET365-API-TOKEN").value
run_id        = os.environ.get("RUN_ID", "unknown")

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
pq     = svc.get_container_client("process-queue")
gold   = svc.get_container_client("gold")

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

# ---------------------------------------------------------------------------
# 1. Read event IDs from the in-progress file written by read_pending_queue
# ---------------------------------------------------------------------------

try:
    raw = json.loads(pq.get_blob_client(f"in-progress/{run_id}.json").download_blob().readall())
    event_ids = list({e["event_id"] for e in (raw.get("entries") or []) if e.get("event_id")})
except Exception as exc:
    print(f"[capture_final_scores] Could not read in-progress file: {exc}")
    event_ids = []

print(f"Events to capture final scores for: {len(event_ids)}")

if not event_ids:
    print("Nothing to do — exiting.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# 2. Capture final score for each event in parallel
# ---------------------------------------------------------------------------

captured = skipped = failed = 0

def _capture_one(eid: str) -> dict:
    blob_path = f"betsapi/event_final/event_id={eid}/event_view.json"
    try:
        r = _requests.get(
            "https://api.b365api.com/v1/event/view",
            params={"event_id": eid, "token": betsapi_token},
            timeout=10,
        )
        payload = r.json()
        results = payload.get("results") or []
        if results and str(results[0].get("time_status") or "") == "3" and results[0].get("ss"):
            bronze.get_blob_client(blob_path).upload_blob(
                json.dumps(payload, ensure_ascii=False, indent=2).encode(),
                overwrite=True,
            )
            return {"eid": eid, "status": "captured", "ss": results[0]["ss"]}
        else:
            ts = results[0].get("time_status") if results else "no results"
            ss = results[0].get("ss", "") if results else ""
            return {"eid": eid, "status": "skipped", "reason": f"time_status={ts!r} ss={ss!r}"}
    except Exception as exc:
        return {"eid": eid, "status": "failed", "reason": str(exc)}

with ThreadPoolExecutor(max_workers=10) as ex:
    futs = {ex.submit(_capture_one, eid): eid for eid in event_ids}
    for fut in as_completed(futs):
        res = fut.result()
        if res["status"] == "captured":
            captured += 1
        elif res["status"] == "skipped":
            skipped += 1
            print(f"  [SKIP] {res['eid']}: {res.get('reason', '')}")
        else:
            failed += 1
            print(f"  [FAIL] {res['eid']}: {res.get('reason', '')}")

elapsed             = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)

print(f"\n── Done ──")
print(f"  Captured : {captured}")
print(f"  Skipped  : {skipped}")
print(f"  Failed   : {failed}")
print(f"  Duration : {elapsed:.1f}s")

# ---------------------------------------------------------------------------
# 3. Write run log
# ---------------------------------------------------------------------------

try:
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    gold.get_blob_client(
        f"logs/pl_build_ended_match/{log_date}/{log_time}_capture_final_scores.json"
    ).upload_blob(json.dumps({
        "script":           "capture_final_scores",
        "run_id":           run_id,
        "started_at_utc":   script_start_utc.isoformat(),
        "finished_at_utc":  script_finished_utc.isoformat(),
        "duration_seconds": round(elapsed, 2),
        "events_total":     len(event_ids),
        "captured":         captured,
        "skipped":          skipped,
        "failed":           failed,
    }, indent=2).encode(), overwrite=True)
except Exception as log_exc:
    print(f"  [log write failed — non-fatal]: {log_exc}")
