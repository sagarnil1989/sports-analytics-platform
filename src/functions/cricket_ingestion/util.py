import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


# ------------------------------------------------------------------
# Market helpers
# ------------------------------------------------------------------

def _is_innings_market(name: str, batting_team: Optional[str] = None, total_overs: int = 20) -> bool:
    """Return True only for the full-innings runs market of the batting team.

    When batting_team is provided (always preferred), checks for the exact
    market name pattern:  "{batting_team} {total_overs} Overs Runs"
    e.g. "Gujarat Titans 20 Overs Runs"

    Falls back to a simple suffix check when batting_team is unknown.
    """
    if batting_team:
        expected = f"{batting_team.strip().lower()} {total_overs} overs runs"
        return name.strip().lower() == expected
    return bool(re.search(r'\b(?:[1-9]\d)\s+overs?\s+runs?\b', name, re.IGNORECASE))


# ------------------------------------------------------------------
# Time helpers
# ------------------------------------------------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_unix_ts(ts: Optional[Any]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return None


def ts_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


# ------------------------------------------------------------------
# Environment helpers
# ------------------------------------------------------------------

def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def get_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


# ------------------------------------------------------------------
# Blob storage helpers
# ------------------------------------------------------------------

def get_blob_service_client() -> BlobServiceClient:
    storage_conn = get_env("DATA_STORAGE_CONNECTION_STRING")
    return BlobServiceClient.from_connection_string(storage_conn)


def get_named_container_client(container_name: str):
    blob_service = get_blob_service_client()
    container = blob_service.get_container_client(container_name)
    try:
        container.create_container()
    except ResourceExistsError:
        pass
    except Exception:
        pass
    return container


def get_bronze_container_client():
    return get_named_container_client("bronze")


def upload_json(container_client, blob_path: str, payload: Dict[str, Any], overwrite: bool = False) -> None:
    container_client.upload_blob(
        name=blob_path,
        data=json.dumps(payload, indent=2, ensure_ascii=False),
        overwrite=overwrite,
        content_settings=ContentSettings(content_type="application/json"),
    )


def download_json(container_client, blob_path: str) -> Optional[Dict[str, Any]]:
    try:
        data = container_client.download_blob(blob_path).readall()
        return json.loads(data)
    except ResourceNotFoundError:
        return None


def download_required_json(container_client, blob_path: str) -> Dict[str, Any]:
    data = container_client.download_blob(blob_path).readall()
    return json.loads(data)


def blob_exists(container_client, blob_path: str) -> bool:
    try:
        container_client.get_blob_client(blob_path).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False


# ------------------------------------------------------------------
# Numeric / type helpers
# ------------------------------------------------------------------

def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


# ------------------------------------------------------------------
# BetsAPI client
# ------------------------------------------------------------------

def call_betsapi(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    base_url = get_env("BETS_API_BASE_URL", "https://api.b365api.com").rstrip("/")
    token = get_env("BETS_API_TOKEN")
    url = f"{base_url}{path}"
    query = dict(params)
    query["token"] = token
    started = utc_now()
    try:
        response = requests.get(url, params=query, timeout=8)
        elapsed_ms = int((utc_now() - started).total_seconds() * 1000)
        try:
            body = response.json()
        except ValueError:
            body = {"raw_text": response.text}
        success = response.status_code == 200 and isinstance(body, dict) and body.get("success") in [1, "1", True]
        return {
            "request": {
                "url": url,
                "params_without_token": {k: v for k, v in query.items() if k != "token"},
                "called_at_utc": started.isoformat(),
            },
            "response": {
                "http_status_code": response.status_code,
                "elapsed_ms": elapsed_ms,
                "success": success,
                "error": body.get("error") if isinstance(body, dict) else None,
                "error_detail": body.get("error_detail") if isinstance(body, dict) else None,
                "body": body,
            },
        }
    except requests.RequestException as ex:
        elapsed_ms = int((utc_now() - started).total_seconds() * 1000)
        logging.exception("BetsAPI request failed")
        return {
            "request": {
                "url": url,
                "params_without_token": {k: v for k, v in query.items() if k != "token"},
                "called_at_utc": started.isoformat(),
            },
            "response": {
                "http_status_code": None,
                "elapsed_ms": elapsed_ms,
                "success": False,
                "error": "REQUEST_EXCEPTION",
                "error_detail": str(ex),
                "body": None,
            },
        }


def extract_results(api_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = api_payload.get("response", {}).get("body")
    if not isinstance(body, dict):
        return []
    results = body.get("results")
    if isinstance(results, list):
        return results
    return []
