"""
Azure Batch script: hypothesis_inn1_prematch
Scans all ended T20 matches, compares the bet365 pre-match "1st Innings Score"
Over/Under line against the actual innings-1 total, writes results to gold.

Environment variables set by ADF Custom activity extendedProperties:
  KEY_VAULT_URI  — e.g. https://kv-ramanuj.vault.azure.net/

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

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

os.environ["DATA_STORAGE_CONNECTION_STRING"] = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
os.environ["SPORT_ID"]                       = _kv.get_secret("SPORT-ID").value

from hypothesis import extract_inn1_prematch_over
from util import get_named_container_client, upload_json

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

result = extract_inn1_prematch_over()

gold = get_named_container_client("gold")
upload_json(gold, "cricket/hypothesis/inn1_prematch_over.json", result, overwrite=True)

elapsed             = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)

print(f"T20 matches scanned      : {result['total_t20_matches']}")
print(f"No pre-match market      : {result['no_prematch_market_count']}")
print(f"Eligible (line available): {result['eligible_matches']}")
print(f"OVER                     : {result['over_count']}")
print(f"UNDER                    : {result['under_count']}")
print(f"PUSH                     : {result['push_count']}")
print(f"OVER %                   : {result['over_pct']}%")

try:
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    log_path = f"logs/pl_build_ended_match/{log_date}/{log_time}_hypothesis_inn1_prematch.json"
    run_log  = {
        "script":                  "hypothesis_inn1_prematch",
        "run_date":                script_start_utc.strftime("%Y-%m-%d"),
        "started_at_utc":          script_start_utc.isoformat(),
        "finished_at_utc":         script_finished_utc.isoformat(),
        "duration_seconds":        round(elapsed, 2),
        "status":                  "ok",
        "total_t20_matches":       result.get("total_t20_matches"),
        "no_prematch_market_count": result.get("no_prematch_market_count"),
        "eligible_matches":        result.get("eligible_matches"),
        "over_count":              result.get("over_count"),
        "under_count":             result.get("under_count"),
        "over_pct":                result.get("over_pct"),
    }
    upload_json(gold, log_path, run_log, overwrite=True)
    print(f"\n  run log written: {log_path}")
except Exception as log_ex:
    print(f"\n  [log write failed — non-fatal]: {log_ex}")
