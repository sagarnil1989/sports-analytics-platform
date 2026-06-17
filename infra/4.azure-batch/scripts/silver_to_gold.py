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

import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

_kv_uri    = os.environ["KEY_VAULT_URI"]
_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
_cred      = ManagedIdentityCredential(client_id=_client_id)
_kv        = SecretClient(vault_url=_kv_uri, credential=_cred)

os.environ["DATA_STORAGE_CONNECTION_STRING"] = _kv.get_secret("DATA-STORAGE-CONNECTION-STRING").value
os.environ["SPORT_ID"]                       = _kv.get_secret("SPORT-ID").value

event_id = os.environ.get("EVENT_ID", "").strip() or None
print(f"[silver_to_gold] event_id={'ALL stale matches' if not event_id else event_id}")

from gold_rebuild import gold_rebuild_ended_matches
gold_rebuild_ended_matches(event_id=event_id)
