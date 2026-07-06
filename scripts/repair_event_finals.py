"""
One-time repair script: fix all event_final blobs with missing or stale scores.

Usage:
  python3 scripts/repair_event_finals.py

Requires environment variables (or edit the values below):
  DATA_STORAGE_CONNECTION_STRING  — Azure Storage connection string
  BETSAPI_TOKEN                   — BetsAPI token

Get the connection string from Key Vault:
  az login
  CONNECTION_STRING=$(az keyvault secret show --vault-name kv-ramanuj \
      --name DATA-STORAGE-CONNECTION-STRING --query value -o tsv)
  export DATA_STORAGE_CONNECTION_STRING=$CONNECTION_STRING
  export BETSAPI_TOKEN=162910-KsjYPbwBieot6s
  python3 scripts/repair_event_finals.py
"""

import json, os, time, sys
import requests
from azure.storage.blob import BlobServiceClient

# ── Config ────────────────────────────────────────────────────────────────────

CONN_STR     = os.environ.get("DATA_STORAGE_CONNECTION_STRING", "")
BETSAPI_TOKEN = os.environ.get("BETSAPI_TOKEN", "162910-KsjYPbwBieot6s")
BRONZE       = "bronze"
GOLD         = "gold"
DRY_RUN      = "--dry-run" in sys.argv   # pass --dry-run to preview without writing

if not CONN_STR:
    print("ERROR: DATA_STORAGE_CONNECTION_STRING not set.")
    print("Run:")
    print("  az login")
    print("  export DATA_STORAGE_CONNECTION_STRING=$(az keyvault secret show \\")
    print("      --vault-name kv-ramanuj --name DATA-STORAGE-CONNECTION-STRING \\")
    print("      --query value -o tsv)")
    sys.exit(1)

svc    = BlobServiceClient.from_connection_string(CONN_STR)
bronze = svc.get_container_client(BRONZE)
gold   = svc.get_container_client(GOLD)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dl(container, path):
    try:
        return json.loads(container.get_blob_client(path).download_blob().readall())
    except Exception:
        return None


def _ul(container, path, data):
    container.get_blob_client(path).upload_blob(
        json.dumps(data, indent=2).encode(), overwrite=True
    )


def _is_valid(payload):
    """True only if event_final has time_status=3 AND non-empty ss."""
    if not payload:
        return False
    ef_body  = (payload.get("response") or {}).get("body") or {}
    results  = ef_body.get("results") or payload.get("results") or []
    if not results:
        return False
    r = results[0]
    return str(r.get("time_status") or "") == "3" and bool(r.get("ss"))


def _fetch(eid):
    try:
        r = requests.get(
            "https://api.b365api.com/v1/event/view",
            params={"event_id": eid, "token": BETSAPI_TOKEN},
            timeout=10,
        )
        return r.json()
    except Exception as e:
        print(f"  [ERROR] API call failed for {eid}: {e}")
        return None


# ── Scan all event_final blobs ────────────────────────────────────────────────

print("Scanning bronze/betsapi/event_final/ ...")
prefix = "betsapi/event_final/event_id="

all_blobs = [
    b.name for b in bronze.list_blobs(name_starts_with=prefix)
    if b.name.endswith("/event_view.json")
]
print(f"Found {len(all_blobs)} event_final blobs\n")

total = skipped = repaired = api_not_ended = failed = 0
repaired_list = []

for blob_name in sorted(all_blobs):
    parts    = blob_name.split("/")
    eid_part = next((p for p in parts if p.startswith("event_id=")), None)
    if not eid_part:
        continue
    eid = eid_part[9:]
    total += 1

    existing = _dl(bronze, blob_name)
    if _is_valid(existing):
        skipped += 1
        continue

    # Bad data — show what we have
    old_ss = "MISSING"
    if existing:
        ef_body  = (existing.get("response") or {}).get("body") or {}
        results  = ef_body.get("results") or existing.get("results") or []
        if results:
            old_ss = results[0].get("ss") or "(empty)"

    print(f"  NEEDS REPAIR: {eid}  current ss={old_ss!r}")

    if DRY_RUN:
        print(f"    [dry-run] would re-fetch")
        repaired_list.append(eid)
        repaired += 1
        continue

    time.sleep(0.15)   # gentle rate-limit respect
    payload = _fetch(eid)
    if not payload:
        failed += 1
        continue

    results    = payload.get("results") or []
    result     = results[0] if results else {}
    time_status = str(result.get("time_status") or "")
    ss         = result.get("ss") or ""

    if not results or time_status != "3" or not ss:
        print(f"    [SKIP] API returned time_status={time_status!r}  ss={ss!r}")
        api_not_ended += 1
        continue

    _ul(bronze, blob_name, payload)
    repaired += 1
    repaired_list.append(eid)
    print(f"    [FIXED] new ss={ss!r}")

# ── Summary ───────────────────────────────────────────────────────────────────

print(f"""
{'='*60}
REPAIR COMPLETE{'  [DRY RUN — nothing written]' if DRY_RUN else ''}
  Total scanned   : {total}
  Already valid   : {skipped}
  Repaired        : {repaired}
  API not ended   : {api_not_ended}
  Failed          : {failed}
{'  Event IDs fixed : ' + ', '.join(repaired_list) if repaired_list else ''}
{'='*60}
""")

if repaired and not DRY_RUN:
    print("Next step: re-run pl_build_ended_match in ADF to rebuild the ended")
    print("index with the corrected scores. All fixed matches will update in")
    print("the ended view and win predictor automatically.")
