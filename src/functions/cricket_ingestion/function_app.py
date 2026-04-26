import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

app = func.FunctionApp()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


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


def get_container_client():
    storage_conn = get_env("DATA_STORAGE_CONNECTION_STRING")
    blob_service = BlobServiceClient.from_connection_string(storage_conn)
    return blob_service.get_container_client("bronze")


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


def get_event_id_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    for key in ["our_event_id", "event_id", "id"]:
        value = item.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def get_fi_from_inplay_item(item: Dict[str, Any]) -> Optional[str]:
    # For Bet365 inplay_filter, docs say the returned id can be used as FI.
    # Some responses also contain FI explicitly.
    for key in ["FI", "fi", "id"]:
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
        live.append({
            "fi": fi,
            "event_id": event_id or fi,
            "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
            "league": item.get("league"),
            "home": item.get("home"),
            "away": item.get("away"),
            "time_status": item.get("time_status"),
            "raw_item": item,
        })
    return live[:max_live_matches]


@app.timer_trigger(
    schedule="*/15 * * * * *",
    arg_name="timer",
    run_on_startup=True,
    use_monitor=False,
)
def discover_cricket_inplay(timer: func.TimerRequest) -> None:
    """Discover live cricket matches and write latest active FI list to bronze/control."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_live_matches = get_int_env("MAX_LIVE_MATCHES", 1)
    container = get_container_client()

    api_payload = call_betsapi(
        path="/v1/bet365/inplay_filter",
        params={"sport_id": sport_id},
    )

    raw_path = (
        f"betsapi/inplay_filter/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"inplay_filter_{ts_compact(now)}.json"
    )
    upload_json(container, raw_path, api_payload)

    active_matches = summarize_inplay_items(extract_results(api_payload), max_live_matches)
    control_payload = {
        "generated_at_utc": now.isoformat(),
        "sport_id": sport_id,
        "max_live_matches": max_live_matches,
        "active_match_count": len(active_matches),
        "active_matches": active_matches,
        "source_raw_path": f"bronze/{raw_path}",
    }

    upload_json(container, "betsapi/control/active_inplay_fi/latest.json", control_payload, overwrite=True)

    logging.info(json.dumps({
        "event": "discover_cricket_inplay_completed",
        "success": api_payload["response"]["success"],
        "active_match_count": len(active_matches),
        "raw_path": f"bronze/{raw_path}",
    }))


@app.timer_trigger(
    schedule="*/5 * * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def capture_cricket_inplay_snapshot(timer: func.TimerRequest) -> None:
    """Capture synchronized match state + Bet365 odds snapshot for active live cricket matches."""
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    container = get_container_client()

    control = download_json(container, "betsapi/control/active_inplay_fi/latest.json")
    if not control or not control.get("active_matches"):
        logging.info(json.dumps({
            "event": "capture_cricket_inplay_snapshot_skipped",
            "reason": "no_active_matches",
        }))
        return

    active_matches = control["active_matches"]
    enable_events_inplay = get_bool_env("ENABLE_EVENTS_INPLAY_MATCH_STATE", True)

    # One clean match-state call shared for all active matches in this snapshot.
    events_inplay_payload = None
    if enable_events_inplay:
        events_inplay_payload = call_betsapi(
            path="/v3/events/inplay",
            params={"sport_id": sport_id},
        )

    for match in active_matches:
        snapshot_time = utc_now()
        snapshot_id = ts_compact(snapshot_time)
        fi = str(match["fi"])
        event_id = str(match.get("event_id") or fi)

        bet365_event_payload = call_betsapi(
            path="/v1/bet365/event",
            params={"FI": fi, "stats": 1},
        )

        base_path = (
            f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/"
            f"snapshot_id={snapshot_id}"
        )

        manifest = {
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time.isoformat(),
            "sport_id": sport_id,
            "event_id": event_id,
            "fi": fi,
            "match_from_filter": match,
            "files": {
                "events_inplay": f"bronze/{base_path}/events_inplay.json" if events_inplay_payload else None,
                "bet365_event": f"bronze/{base_path}/bet365_event.json",
            },
            "status": {
                "events_inplay_success": events_inplay_payload["response"]["success"] if events_inplay_payload else None,
                "bet365_event_success": bet365_event_payload["response"]["success"],
                "bet365_event_error": bet365_event_payload["response"].get("error"),
                "bet365_event_error_detail": bet365_event_payload["response"].get("error_detail"),
            },
        }

        if events_inplay_payload:
            upload_json(container, f"{base_path}/events_inplay.json", events_inplay_payload)

        upload_json(container, f"{base_path}/bet365_event.json", bet365_event_payload)
        upload_json(container, f"{base_path}/manifest.json", manifest)

        logging.info(json.dumps({
            "event": "capture_cricket_inplay_snapshot_completed",
            "snapshot_id": snapshot_id,
            "event_id": event_id,
            "fi": fi,
            "bet365_event_success": bet365_event_payload["response"]["success"],
            "base_path": f"bronze/{base_path}",
        }))
