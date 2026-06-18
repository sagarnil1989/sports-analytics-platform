"""
Azure Batch script: hypothesis_inn2_over6
Scans all ended matches, finds the innings-2 over-6 snapshot for each,
records match-winner odds and actual winner, writes results to gold.

Environment variables set by ADF Custom activity extendedProperties:
  KEY_VAULT_URI  — e.g. https://kv-ramanuj.vault.azure.net/

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
"""

import os, sys, json, time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

os.environ["DATA_STORAGE_CONNECTION_STRING"] = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
os.environ["SPORT_ID"]                       = _kv.get_secret("SPORT-ID").value

from hypothesis import extract_inn2_over6_favorite
from util import get_named_container_client, upload_json

script_start_utc = datetime.now(timezone.utc)
run_start        = time.monotonic()

result = extract_inn2_over6_favorite()

gold = get_named_container_client("gold")
upload_json(gold, "cricket/hypothesis/inn2_over6_favorite.json", result, overwrite=True)

elapsed             = time.monotonic() - run_start
script_finished_utc = datetime.now(timezone.utc)

print(f"Matches processed : {result['total_matches']}")
print(f"Eligible (odds available): {result['eligible_matches']}")
print(f"Favourite won     : {result['favorite_won_count']}")
print(f"Favourite lost    : {result['favorite_lost_count']}")
print(f"No odds data      : {result['no_odds_data_count']}")
print(f"Win %             : {result['favorite_win_pct']}%")

try:
    log_date = script_start_utc.strftime("%Y%m%d")
    log_time = script_start_utc.strftime("%H%M%S")
    log_path = f"logs/pl_build_ended_match/{log_date}/{log_time}_hypothesis_inn2_over6.json"
    run_log  = {
        "script":               "hypothesis_inn2_over6",
        "run_date":             script_start_utc.strftime("%Y-%m-%d"),
        "started_at_utc":       script_start_utc.isoformat(),
        "finished_at_utc":      script_finished_utc.isoformat(),
        "duration_seconds":     round(elapsed, 2),
        "status":               "ok",
        "total_matches":        result.get("total_matches"),
        "eligible_matches":     result.get("eligible_matches"),
        "favorite_won_count":   result.get("favorite_won_count"),
        "favorite_lost_count":  result.get("favorite_lost_count"),
        "no_odds_data_count":   result.get("no_odds_data_count"),
        "favorite_win_pct":     result.get("favorite_win_pct"),
    }
    upload_json(gold, log_path, run_log, overwrite=True)
    print(f"\n  run log written: {log_path}")
except Exception as log_ex:
    print(f"\n  [log write failed — non-fatal]: {log_ex}")
