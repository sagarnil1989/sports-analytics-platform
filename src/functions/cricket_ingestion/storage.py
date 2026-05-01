import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError


# Innings market detection shared across modules
_INNINGS_MARKET_RE = re.compile(
    r'\b\d{2,}\s+overs?\s+runs?\b'                                       # "20 Overs Runs", "50 Overs Runs" — 2+ digits excludes "2nd Over", "Next 3 Overs"
    r'|'
    r'\bruns?\s+in\s+\d{2,}\s+overs?\b'                                  # "Runs in 20 Overs"
    r'|'
    r'\b(?:1st|2nd|3rd|4th|first|second|current)\s+inn(?:ing|ings)?\s+runs?\b',  # "1st Innings Runs", "Current Innings Runs"
    re.IGNORECASE,
)
# "innings run" removed — it is a substring of player markets like "Sharmin Akhter Innings Runs"
_INNINGS_KEYWORDS = ["current inn", "1st inn", "inning run"]


def _is_innings_market(name: str) -> bool:
    n = name.lower()
    if any(kw in n for kw in _INNINGS_KEYWORDS):
        return True
    return bool(_INNINGS_MARKET_RE.search(name))


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
        response = requests.get(url, params=query, timeout=20)
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


# ------------------------------------------------------------------
# Inplay match summarization
# ------------------------------------------------------------------

def get_event_id_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ["our_event_id", "event_id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    if item.get("bet365_id") is not None:
        value = item.get("id")
        if value is not None and str(value).strip():
            return str(value)
    value = item.get("id")
    if value is not None and str(value).strip():
        return str(value)
    return None


def get_fi_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ["bet365_id", "FI", "fi", "id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def summarize_inplay_items(items: List[Dict[str, Any]], max_live_matches: int) -> List[Dict[str, Any]]:
    live = []
    for item in items:
        fi = get_fi_from_inplay_item(item)
        event_id = get_event_id_from_inplay_item(item)
        if not fi:
            continue
        if str(item.get("time_status", "1")) != "1":
            continue
        _league = item.get("league") or {}
        live.append({
            "fi": fi,
            "event_id": event_id or fi,
            "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
            "league_id": str(_league.get("id")) if _league.get("id") is not None else None,
            "league_name": _league.get("name"),
            "league": _league,
            "home": item.get("home"),
            "away": item.get("away"),
            "time_status": item.get("time_status"),
            "raw_item": item,
        })
    if max_live_matches <= 0:
        return live
    return live[:max_live_matches]


# ------------------------------------------------------------------
# Lineage helpers
# ------------------------------------------------------------------

def get_api_result_count(api_payload: Optional[Dict[str, Any]]) -> Optional[int]:
    if not api_payload:
        return None
    body = api_payload.get("response", {}).get("body")
    if not isinstance(body, dict):
        return None
    results = body.get("results")
    if isinstance(results, list):
        return len(results)
    if isinstance(results, dict):
        return len(results)
    return None


def build_api_call_lineage(
    api_name: str,
    api_payload: Optional[Dict[str, Any]],
    id_used: Dict[str, Any],
    bronze_path: Optional[str],
    purpose: str,
) -> Dict[str, Any]:
    response = (api_payload or {}).get("response", {})
    request = (api_payload or {}).get("request", {})
    return {
        "api_name": api_name,
        "purpose": purpose,
        "id_used": id_used,
        "path": request.get("url"),
        "params_without_token": request.get("params_without_token"),
        "called_at_utc": request.get("called_at_utc"),
        "http_status_code": response.get("http_status_code"),
        "success": response.get("success"),
        "elapsed_ms": response.get("elapsed_ms"),
        "error": response.get("error"),
        "error_detail": response.get("error_detail"),
        "result_count": get_api_result_count(api_payload),
        "bronze_path": bronze_path,
    }


def build_live_snapshot_lineage(
    sport_id: str,
    event_id: str,
    fi: str,
    base_path: str,
    payloads: Dict[str, Optional[Dict[str, Any]]],
) -> Dict[str, Any]:
    bronze_base = f"bronze/{base_path}"
    calls = [
        build_api_call_lineage("events_inplay", payloads.get("events_inplay"), {"sport_id": sport_id}, f"{bronze_base}/api_inplay_event_list.json", "Find live cricket matches and current score/status from /v3/events/inplay."),
        build_api_call_lineage("event_view", payloads.get("event_view"), {"event_id": event_id}, f"{bronze_base}/api_event_view.json", "Get event scoreboard/details from /v1/event/view using event_id."),
        build_api_call_lineage("event_odds_summary", payloads.get("event_odds_summary"), {"event_id": event_id}, f"{bronze_base}/api_event_odds_summary.json", "Get compact odds summary from /v2/event/odds/summary using event_id."),
        build_api_call_lineage("event_odds", payloads.get("event_odds"), {"event_id": event_id}, f"{bronze_base}/api_event_odds.json", "Get event odds history from /v2/event/odds using event_id."),
        build_api_call_lineage("bet365_event", payloads.get("bet365_event"), {"FI": fi}, f"{bronze_base}/api_live_market_odds.json", "Get live Bet365 market stream from /v1/bet365/event using FI/bet365_id."),
    ]
    return {
        "generated_at_utc": utc_now().isoformat(),
        "sport_id": sport_id,
        "event_id": event_id,
        "fi": fi,
        "id_mapping": {
            "event_id": {"value": event_id, "used_for": ["/v1/event/view", "/v2/event/odds/summary", "/v2/event/odds"]},
            "fi": {"value": fi, "also_called": "bet365_id", "used_for": ["/v1/bet365/event"]},
        },
        "bronze_base_path": f"bronze/{base_path}",
        "api_calls": calls,
    }
