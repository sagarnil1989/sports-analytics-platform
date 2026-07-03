"""
Azure Batch script: hypothesis_odds_movement
Scans all ended T20 matches, computes in-play odds swing metrics per match,
and aggregates by league and team. Writes results to gold.

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

from odds_movement import extract_odds_movement
from util import get_named_container_client, upload_json

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

result = extract_odds_movement()

gold = get_named_container_client("gold")
upload_json(gold, "cricket/analysis/odds_movement_summary.json", result, overwrite=True)

elapsed             = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)

print(f"Matches processed      : {result['total_matches']}")
print(f"Double-opportunity     : {result['double_opportunity_count']}")
print(f"Avg swing              : {result['avg_swing']}")
print(f"Max swing (any match)  : {result['max_swing_ever']}")

try:
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    log_path = f"logs/pl_ml_and_hypothesis/{log_date}/{log_time}_hypothesis_odds_movement.json"
    upload_json(gold, log_path, {
        "script":               "hypothesis_odds_movement",
        "started_at_utc":       script_start_utc.isoformat(),
        "finished_at_utc":      script_finished_utc.isoformat(),
        "duration_seconds":     round(elapsed, 2),
        "status":               "ok",
        "total_matches":        result.get("total_matches"),
        "double_opportunity_count": result.get("double_opportunity_count"),
        "avg_swing":            result.get("avg_swing"),
        "max_swing_ever":       result.get("max_swing_ever"),
    }, overwrite=True)
    print(f"\n  run log written: {log_path}")
except Exception as log_ex:
    print(f"\n  [log write failed — non-fatal]: {log_ex}")
