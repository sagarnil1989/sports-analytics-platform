"""
Azure Batch script: hypothesis_timeout_wicket
Scans all ended matches, detects strategic timeouts (game paused > 2 min),
checks whether a wicket fell in the over that immediately resumed.

Environment variables set by ADF Custom activity extendedProperties:
  KEY_VAULT_URI  — e.g. https://kv-ramanuj.vault.azure.net/

KV secrets read at startup:
  DATA-STORAGE-CONNECTION-STRING
  SPORT-ID
"""

import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

_kv_uri = os.environ["KEY_VAULT_URI"]
_cred   = ManagedIdentityCredential()
_kv     = SecretClient(vault_url=_kv_uri, credential=_cred)

os.environ["DATA_STORAGE_CONNECTION_STRING"] = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
os.environ["SPORT_ID"]                       = _kv.get_secret("SPORT-ID").value

from hypothesis import extract_timeout_wicket
from util import get_named_container_client, upload_json

result = extract_timeout_wicket()

gold = get_named_container_client("gold")
upload_json(gold, "cricket/hypothesis/timeout_wicket.json", result, overwrite=True)

print(f"Total timeouts detected : {result['total_timeouts_detected']}")
print(f"Eligible (over data)    : {result['eligible_timeouts']}")
print(f"Wicket in resumed over  : {result['wicket_in_resumed_over_count']}")
print(f"No wicket               : {result['no_wicket_count']}")
print(f"Unknown                 : {result['unknown_count']}")
print(f"Wicket %                : {result['wicket_pct']}%")
