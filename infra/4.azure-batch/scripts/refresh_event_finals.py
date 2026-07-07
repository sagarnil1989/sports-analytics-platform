"""
Azure Batch script: refresh_event_finals
Called between silver_to_gold and discover_cricket_ended in pl_build_ended_match.

Scans all gold event_id= folders for innings_tracker.json files (same set that
discover_cricket_ended will process). For each event it calls /v1/event/view and
overwrites the bronze event_final blob with the fresh API response, provided the
API returns time_status=3 with a non-empty ss.

If the API call fails or returns an incomplete result, the existing blob is left
untouched so that discover_cricket_ended can still fall back to it.

Environment variables (set by ADF extendedProperties):
  KEY_VAULT_URI               — e.g. https://kv-ramanuj.vault.azure.net/
  MANAGED_IDENTITY_CLIENT_ID  — user-assigned identity for the Batch pool
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
sport_id      = _kv.get_secret("SPORT-ID").value
betsapi_token = _kv.get_secret("BET365-API-TOKEN").value

svc    = BlobServiceClient.from_connection_string(conn_str)
bronze = svc.get_container_client("bronze")
gold   = svc.get_container_client("gold")

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ul(container, path, data):
    container.get_blob_client(path).upload_blob(
        json.dumps(data, ensure_ascii=False, indent=2).encode(),
        overwrite=True,
    )

# ---------------------------------------------------------------------------
# 1. Collect event IDs from gold (same set discover_cricket_ended will process)
# ---------------------------------------------------------------------------

event_ids = []
for blob in gold.list_blobs(name_starts_with="event_id="):
    if not blob.name.endswith("/innings_tracker.json"):
        continue
    parts    = blob.name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    if eid_part:
        event_ids.append(eid_part[9:])

print(f"Events to refresh: {len(event_ids)}")

# Exclude currently live events — no final score yet
live_eids: set = set()
try:
    live_idx = json.loads(gold.get_blob_client("cricket/matches/latest/index.json").download_blob().readall())
    for m in (live_idx.get("matches") or []):
        eid = str(m.get("event_id") or "")
        if eid:
            live_eids.add(eid)
except Exception:
    pass

event_ids = [e for e in event_ids if e not in live_eids]
print(f"After excluding live: {len(event_ids)}  (excluded {len(live_eids)} live)")

# ---------------------------------------------------------------------------
# 2. Refresh each event_final blob in parallel
# ---------------------------------------------------------------------------

refreshed = skipped = failed = 0
results_log = []

def _update_gold_tracker_score(eid: str, ss: str) -> None:
    """Patch score_summary_events in the gold tracker with the authoritative final score."""
    gold_path = f"event_id={eid}/innings_tracker.json"
    try:
        tracker = json.loads(gold.get_blob_client(gold_path).download_blob().readall())
        tracker["score_summary_events"] = ss.replace("-", ",")
        gold.get_blob_client(gold_path).upload_blob(
            json.dumps(tracker, ensure_ascii=False, indent=2).encode(),
            overwrite=True,
        )
    except Exception as exc:
        print(f"  [WARN] could not patch gold tracker for {eid}: {exc}")


def _refresh_one(eid: str) -> dict:
    blob_path = f"betsapi/event_final/event_id={eid}/event_view.json"
    try:
        r = _requests.get(
            "https://api.b365api.com/v1/event/view",
            params={"event_id": eid, "token": betsapi_token},
            timeout=10,
        )
        payload  = r.json()
        results  = payload.get("results") or []
        if results and str(results[0].get("time_status") or "") == "3" and results[0].get("ss"):
            ss = results[0]["ss"]
            _ul(bronze, blob_path, payload)
            _update_gold_tracker_score(eid, ss)
            return {"eid": eid, "status": "refreshed", "ss": ss}
        else:
            ts = results[0].get("time_status") if results else "no results"
            ss = results[0].get("ss") if results else ""
            return {"eid": eid, "status": "skipped", "reason": f"time_status={ts!r} ss={ss!r}"}
    except Exception as exc:
        return {"eid": eid, "status": "failed", "reason": str(exc)}

with ThreadPoolExecutor(max_workers=10) as ex:
    futs = {ex.submit(_refresh_one, eid): eid for eid in event_ids}
    for fut in as_completed(futs):
        res = fut.result()
        results_log.append(res)
        if res["status"] == "refreshed":
            refreshed += 1
        elif res["status"] == "skipped":
            skipped += 1
            print(f"  [SKIP] {res['eid']}: {res.get('reason', '')}")
        else:
            failed += 1
            print(f"  [FAIL] {res['eid']}: {res.get('reason', '')}")

elapsed             = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)

print(f"\n── Done ──")
print(f"  Refreshed   : {refreshed}")
print(f"  Skipped     : {skipped}")
print(f"  Failed      : {failed}")
print(f"  Duration    : {elapsed:.1f}s")

# ---------------------------------------------------------------------------
# 3. Write run log
# ---------------------------------------------------------------------------

try:
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    log_path = f"logs/pl_build_ended_match/{log_date}/{log_time}_refresh_event_finals.json"
    _ul(gold, log_path, {
        "script":            "refresh_event_finals",
        "started_at_utc":    script_start_utc.isoformat(),
        "finished_at_utc":   script_finished_utc.isoformat(),
        "duration_seconds":  round(elapsed, 2),
        "status":            "ok",
        "total":             len(event_ids),
        "refreshed":         refreshed,
        "skipped":           skipped,
        "failed":            failed,
    })
    print(f"\n  run log written: {log_path}")
except Exception as log_exc:
    print(f"\n  [log write failed — non-fatal]: {log_exc}")
