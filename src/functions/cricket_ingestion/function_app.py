import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import azure.functions as func
import requests
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

app = func.FunctionApp()


# -----------------------------
# Common helpers
# -----------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts_compact(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")

def format_unix_ts(value):
    if value is None or value == "":
        return "-"

    try:
        # Bet365 timestamps are Unix seconds
        ts = int(float(value))
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(value)

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
        event_id = item.get("id")
        fi = item.get("bet365_id")

        if not event_id or not fi:
            continue

        live.append({
            "event_id": str(event_id),
            "fi": str(fi),
            "sport_id": str(item.get("sport_id", os.environ.get("SPORT_ID", "3"))),
            "league": item.get("league"),
            "home": item.get("home"),
            "away": item.get("away"),
            "time_status": item.get("time_status"),
            "score": item.get("ss"),
            "raw_item": item,
        })

    if max_live_matches <= 0:
        return live

    return live[:max_live_matches]


# -----------------------------
# Bronze ingestion
# -----------------------------

@app.timer_trigger(schedule="*/15 * * * * *", arg_name="timer", run_on_startup=True, use_monitor=False)
def discover_cricket_inplay(timer: func.TimerRequest) -> None:
    now = utc_now()
    sport_id = get_env("SPORT_ID", "3")
    max_live_matches = get_int_env("MAX_LIVE_MATCHES", 10)
    container = get_bronze_container_client()

    api_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    raw_path = (
        f"betsapi/inplay_filter/sport_id={sport_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
        f"events_inplay_{ts_compact(now)}.json"
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


@app.timer_trigger(schedule="*/5 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def capture_cricket_inplay_snapshot(timer: func.TimerRequest) -> None:
    sport_id = get_env("SPORT_ID", "3")
    container = get_bronze_container_client()

    control = download_json(container, "betsapi/control/active_inplay_fi/latest.json")
    if not control or not control.get("active_matches"):
        logging.info(json.dumps({"event": "capture_cricket_inplay_snapshot_skipped", "reason": "no_active_matches"}))
        return

    active_matches = control["active_matches"]
    enable_events_inplay = get_bool_env("ENABLE_EVENTS_INPLAY_MATCH_STATE", True)

    events_inplay_payload = None
    if enable_events_inplay:
        events_inplay_payload = call_betsapi(path="/v3/events/inplay", params={"sport_id": sport_id})

    for match in active_matches:
        snapshot_time = utc_now()
        snapshot_id = ts_compact(snapshot_time)
        fi = str(match["fi"])
        event_id = str(match.get("event_id") or fi)

        bet365_event_payload = call_betsapi(path="/v1/bet365/event", params={"FI": fi, "stats": 1})
        event_odds_payload = call_betsapi(path="/v2/event/odds", params={"event_id": event_id})

        base_path = f"betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}"
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
                "event_odds": f"bronze/{base_path}/event_odds.json",
            },
            "status": {
                "events_inplay_success": events_inplay_payload["response"]["success"] if events_inplay_payload else None,
                "bet365_event_success": bet365_event_payload["response"]["success"],
                "bet365_event_error": bet365_event_payload["response"].get("error"),
                "bet365_event_error_detail": bet365_event_payload["response"].get("error_detail"),
                "event_odds_success": event_odds_payload["response"]["success"],
                "event_odds_error": event_odds_payload["response"].get("error"),
                "event_odds_error_detail": event_odds_payload["response"].get("error_detail"),
            },
        }

        if events_inplay_payload:
            upload_json(container, f"{base_path}/events_inplay.json", events_inplay_payload)
        upload_json(container, f"{base_path}/bet365_event.json", bet365_event_payload)
        upload_json(container, f"{base_path}/event_odds.json", event_odds_payload)
        upload_json(container, f"{base_path}/manifest.json", manifest)

        logging.info(json.dumps({
            "event": "capture_cricket_inplay_snapshot_completed",
            "snapshot_id": snapshot_id,
            "event_id": event_id,
            "fi": fi,
            "bet365_event_success": bet365_event_payload["response"]["success"],
            "event_odds_success": event_odds_payload["response"]["success"],
            "base_path": f"bronze/{base_path}",
        }))


# -----------------------------
# Silver parsing helpers
# -----------------------------

def list_manifest_paths(bronze_container, sport_id: str, limit: int) -> List[str]:
    prefix = f"betsapi/inplay_snapshot/sport_id={sport_id}/"
    manifests = []
    for blob in bronze_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/manifest.json"):
            manifests.append((getattr(blob, "last_modified", None), name))
    manifests.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in manifests[:limit]]


def parse_runs_wickets(raw: Optional[str]) -> Dict[str, Optional[int]]:
    if not raw or "#" not in raw:
        return {"runs": None, "wickets": None}
    left, right = raw.split("#", 1)
    try:
        runs = int(left.strip())
    except Exception:
        runs = None
    try:
        wickets = int(right.strip())
    except Exception:
        wickets = None
    return {"runs": runs, "wickets": wickets}


def extract_bet365_records(bet365_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = bet365_payload.get("response", {}).get("body", {})
    results = body.get("results", [])
    if results and isinstance(results[0], list):
        return results[0]
    if isinstance(results, list):
        return results
    return []


def find_matching_event(events_inplay_payload: Dict[str, Any], event_id: str, fi: str) -> Optional[Dict[str, Any]]:
    results = extract_results(events_inplay_payload)
    for item in results:
        if str(item.get("id")) == str(event_id):
            return item
        if str(item.get("bet365_id")) == str(fi):
            return item
        if str(item.get("our_event_id")) == str(event_id):
            return item
    return None


def safe_float(value: Any) -> Optional[float]:
    """Convert normal decimal strings to float. For fractional odds, use odds_to_decimal."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def fractional_odds_to_decimal(value: Any) -> Optional[float]:
    """Convert Bet365 fractional odds such as 13/8 or 10/11 to decimal odds.

    Decimal odds = 1 + numerator / denominator.
    """
    if value is None or value == "":
        return None

    raw = str(value).strip()
    if "/" not in raw:
        return safe_float(raw)

    try:
        numerator, denominator = raw.split("/", 1)
        denominator_float = float(denominator)
        if denominator_float == 0:
            return None
        return round(1 + (float(numerator) / denominator_float), 3)
    except Exception:
        return None


def extract_market_template_id_from_it(it_value: Any) -> Optional[str]:
    """Extract template id from Bet365 IT values such as ...-30143-H_1_1."""
    if not it_value:
        return None
    match = re.search(r"-(\d+)-H", str(it_value))
    return match.group(1) if match else None


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value if text_value else None


def extract_current_score_context(match_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    score = match_snapshot.get("score_summary_events") or match_snapshot.get("score_summary_bet365")
    return {
        "score": score,
        "score_summary_events": match_snapshot.get("score_summary_events"),
        "score_summary_bet365": match_snapshot.get("score_summary_bet365"),
    }


def extract_event_odds_records(
    event_odds_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
    score_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    body = event_odds_payload.get("response", {}).get("body", {})
    results = body.get("results", {}) if isinstance(body, dict) else {}
    stats = results.get("stats", {}) if isinstance(results, dict) else {}
    odds_root = results.get("odds", {}) if isinstance(results, dict) else {}
    rows: List[Dict[str, Any]] = []

    if not isinstance(odds_root, dict):
        return rows

    for market_key, odds_list in odds_root.items():
        if not isinstance(odds_list, list):
            continue
        for item in odds_list:
            if not isinstance(item, dict):
                continue
            rows.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "market_key": str(market_key),
                "odds_id": str(item.get("id")) if item.get("id") is not None else None,
                "score_from_match_snapshot": score_context.get("score"),
                "score_from_odds": item.get("ss"),
                "home_odds": item.get("home_od"),
                "home_odds_decimal": safe_float(item.get("home_od")),
                "away_odds": item.get("away_od"),
                "away_odds_decimal": safe_float(item.get("away_od")),
                "draw_odds": item.get("draw_od"),
                "draw_odds_decimal": safe_float(item.get("draw_od")),
                "handicap": item.get("handicap"),
                "over_odds": item.get("over_od"),
                "over_odds_decimal": safe_float(item.get("over_od")),
                "under_odds": item.get("under_od"),
                "under_odds_decimal": safe_float(item.get("under_od")),
                "add_time": item.get("add_time"),
                "odds_update_stats": stats.get("odds_update"),
                "raw": item,
            })
    return rows



def extract_bet365_current_markets(
    bet365_event_payload: Dict[str, Any],
    snapshot_id: str,
    snapshot_time_utc: str,
    event_id: str,
    fi: str,
    score_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Parse all current Bet365 markets from /v1/bet365/event.

    Bet365 event data is a flat ordered stream:
    - MG = market group, e.g. "Match Winner 2-Way"
    - MA = market/column within the group, e.g. "Over" or "Under"
    - PA = selection/price row. PA rows with OD contain real odds.

    Some markets first publish handicap/line values as PA rows without OD
    (for example "0.5", "3.5", "153.5"). We remember those and attach
    them to later PA rows using the MA/template id.
    """
    records = extract_bet365_records(bet365_event_payload)
    rows: List[Dict[str, Any]] = []

    current_group: Dict[str, Any] = {}
    current_market: Dict[str, Any] = {}

    # Reset for every MG. Used for markets like:
    # PA NA=0.5 IT=...-30143-H, then PA OD with MA=30143.
    line_by_template_id: Dict[str, str] = {}
    line_by_order: Dict[str, str] = {}

    for r in records:
        if not isinstance(r, dict):
            continue

        r_type = r.get("type")

        if r_type == "MG":
            current_group = {
                "market_group_id": clean_text(r.get("ID")),
                "market_group_name": clean_text(r.get("NA")) or clean_text(r.get("IT")) or "Unknown Market",
                "market_group_order": clean_text(r.get("OR")),
                "market_group_suspended": clean_text(r.get("SU")),
                "market_group_raw": r,
            }
            current_market = {}
            line_by_template_id = {}
            line_by_order = {}
            continue

        if r_type == "MA":
            ma_name = clean_text(r.get("NA"))
            current_market = {
                "market_id": clean_text(r.get("ID")),
                "market_name": ma_name,
                "market_it": clean_text(r.get("IT")),
                "market_py": clean_text(r.get("PY")),
                "market_sy": clean_text(r.get("SY")),
                "market_order": clean_text(r.get("OR")),
                "market_raw": r,
            }
            continue

        if r_type != "PA":
            continue

        odds_fractional = clean_text(r.get("OD"))

        # PA without OD often carries line/header values, not a price.
        if not odds_fractional:
            line_value = clean_text(r.get("NA"))
            if line_value:
                template_id = clean_text(r.get("MA")) or extract_market_template_id_from_it(r.get("IT"))
                if template_id:
                    line_by_template_id[template_id] = line_value
                order = clean_text(r.get("OR"))
                if order:
                    line_by_order[order] = line_value
            continue

        selection_order = clean_text(r.get("OR"))
        template_id = clean_text(r.get("MA"))
        line_value = clean_text(r.get("HA")) or clean_text(r.get("HD"))
        if not line_value and template_id:
            line_value = line_by_template_id.get(template_id)
        if not line_value and selection_order:
            line_value = line_by_order.get(selection_order)

        market_group_name = current_group.get("market_group_name") or "Unknown Market"
        market_name = current_market.get("market_name")
        selection_name = clean_text(r.get("NA"))

        # For Over/Under markets the PA often has no NA; the MA name is the selection.
        if not selection_name:
            selection_name = market_name or market_group_name

        display_selection_name = selection_name
        if line_value and selection_name and line_value not in selection_name:
            display_selection_name = f"{selection_name} {line_value}"

        suspended_raw = clean_text(r.get("SU"))
        is_suspended = suspended_raw == "0" or current_group.get("market_group_suspended") == "0"

        rows.append({
            "snapshot_id": snapshot_id,
            "snapshot_time_utc": snapshot_time_utc,
            "event_id": event_id,
            "fi": fi,
            "score_from_match_snapshot": score_context.get("score"),

            "record_type": r_type,

            "market_group_id": current_group.get("market_group_id"),
            "market_group_name": market_group_name,
            "market_group_order": current_group.get("market_group_order"),
            "market_group_suspended": current_group.get("market_group_suspended"),

            "market_id": current_market.get("market_id"),
            "market_name": market_name,
            "market_order": current_market.get("market_order"),
            "market_template_id": template_id,

            "selection_id": clean_text(r.get("ID")),
            "selection_name": selection_name,
            "display_selection_name": display_selection_name,
            "selection_order": selection_order,

            "odds_fractional": odds_fractional,
            "odds_decimal": fractional_odds_to_decimal(odds_fractional),

            # Keep old field name for backward compatibility with the HTML page.
            "odds": odds_fractional,

            "line": line_value,
            "handicap": line_value,
            "suspended": is_suspended,
            "suspended_raw": suspended_raw,

            "raw": r,
        })

    return rows

def parse_silver_snapshot(manifest: Dict[str, Any], events_inplay_payload: Dict[str, Any], bet365_event_payload: Dict[str, Any], event_odds_payload: Dict[str, Any]) -> Dict[str, Any]:
    snapshot_id = manifest["snapshot_id"]
    snapshot_time_utc = manifest["snapshot_time_utc"]
    event_id = str(manifest["event_id"])
    fi = str(manifest["fi"])
    sport_id = str(manifest.get("sport_id", "3"))

    records = extract_bet365_records(bet365_event_payload)
    ev = next((r for r in records if r.get("type") == "EV"), {})
    matching_event = find_matching_event(events_inplay_payload, event_id, fi) or {}

    match_from_filter = manifest.get("match_from_filter", {})
    home = matching_event.get("home") or match_from_filter.get("home") or {}
    away = matching_event.get("away") or match_from_filter.get("away") or {}
    league = matching_event.get("league") or match_from_filter.get("league") or {}

    match_snapshot = {
        "snapshot_id": snapshot_id,
        "snapshot_time_utc": snapshot_time_utc,
        "sport_id": sport_id,
        "event_id": event_id,
        "fi": fi,
        "league_id": str(league.get("id")) if league.get("id") is not None else None,
        "league_name": league.get("name") or ev.get("CT"),
        "home_team_id": str(home.get("id")) if home.get("id") is not None else None,
        "home_team_name": home.get("name"),
        "away_team_id": str(away.get("id")) if away.get("id") is not None else None,
        "away_team_name": away.get("name"),
        "match_name": ev.get("NA"),
        "time_status": matching_event.get("time_status") or match_from_filter.get("time_status"),
        "score_summary_events": matching_event.get("ss") or match_from_filter.get("raw_item", {}).get("ss"),
        "score_summary_bet365": ev.get("SS"),
        "bet365_event_success": manifest.get("status", {}).get("bet365_event_success"),
        "events_inplay_success": manifest.get("status", {}).get("events_inplay_success"),
        "event_odds_success": manifest.get("status", {}).get("event_odds_success"),
        "source_bronze_manifest_path": f"bronze/betsapi/inplay_snapshot/sport_id={sport_id}/event_id={event_id}/fi={fi}/snapshot_id={snapshot_id}/manifest.json",
    }

    team_scores: List[Dict[str, Any]] = []
    player_entries: List[Dict[str, Any]] = []

    for r in records:
        r_type = r.get("type")
        if r_type == "TE" and r.get("NA") and (r.get("SC") or r.get("S5")):
            parsed = parse_runs_wickets(r.get("S5"))
            team_scores.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "team_or_player_id": str(r.get("ID")) if r.get("ID") is not None else None,
                "name": r.get("NA"),
                "raw_score_text": r.get("SC"),
                "runs_wickets_raw": r.get("S5"),
                "runs": parsed["runs"],
                "wickets": parsed["wickets"],
                "s1": r.get("S1"),
                "s2": r.get("S2"),
                "s3": r.get("S3"),
                "s4": r.get("S4"),
                "s6": r.get("S6"),
                "s7": r.get("S7"),
                "s8": r.get("S8"),
                "raw": r,
            })
        elif r_type == "TE" and r.get("NA"):
            player_entries.append({
                "snapshot_id": snapshot_id,
                "snapshot_time_utc": snapshot_time_utc,
                "event_id": event_id,
                "fi": fi,
                "player_id": str(r.get("ID")) if r.get("ID") is not None else None,
                "player_name": r.get("NA"),
                "order": r.get("OR"),
                "raw": r,
            })

    score_context = extract_current_score_context(match_snapshot)
    odds_records = extract_event_odds_records(event_odds_payload, snapshot_id, snapshot_time_utc, event_id, fi, score_context)
    current_markets = extract_bet365_current_markets(
        bet365_event_payload,
        snapshot_id,
        snapshot_time_utc,
        event_id,
        fi,
        score_context,
    )

    return {
        "match_snapshot": match_snapshot,
        "team_scores": team_scores,
        "player_entries": player_entries,
        "odds_records": odds_records,
        "current_markets": current_markets,
    }


def write_silver_outputs(silver_container, parsed: Dict[str, Any]) -> None:
    match = parsed["match_snapshot"]
    dt = datetime.fromisoformat(match["snapshot_time_utc"].replace("Z", "+00:00"))
    base = (
        f"cricket/inplay/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/"
        f"event_id={match['event_id']}/snapshot_id={match['snapshot_id']}"
    )
    upload_json(silver_container, f"{base}/match_snapshot.json", parsed["match_snapshot"], overwrite=True)
    upload_json(silver_container, f"{base}/team_scores.json", {"rows": parsed["team_scores"]}, overwrite=True)
    upload_json(silver_container, f"{base}/player_entries.json", {"rows": parsed["player_entries"]}, overwrite=True)
    upload_json(silver_container, f"{base}/odds_records.json", {"rows": parsed["odds_records"]}, overwrite=True)
    upload_json(silver_container, f"{base}/current_markets.json", {"rows": parsed.get("current_markets", [])}, overwrite=True)

    marker_path = f"cricket/control/processed_snapshots/{match['snapshot_id']}_{match['event_id']}_{match['fi']}.json"
    marker = {
        "processed_at_utc": utc_now().isoformat(),
        "snapshot_id": match["snapshot_id"],
        "event_id": match["event_id"],
        "fi": match["fi"],
        "silver_base_path": f"silver/{base}",
        "team_score_count": len(parsed["team_scores"]),
        "player_entry_count": len(parsed["player_entries"]),
        "odds_record_count": len(parsed["odds_records"]),
        "current_market_selection_count": len(parsed.get("current_markets", [])),
    }
    upload_json(silver_container, marker_path, marker, overwrite=True)


@app.timer_trigger(schedule="*/30 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def parse_cricket_bronze_to_silver(timer: func.TimerRequest) -> None:
    sport_id = get_env("SPORT_ID", "3")
    max_per_run = get_int_env("MAX_SILVER_SNAPSHOTS_PER_RUN", 50)
    bronze = get_named_container_client("bronze")
    silver = get_named_container_client("silver")
    manifest_paths = list_manifest_paths(bronze, sport_id, max_per_run)
    processed = skipped = failed = 0

    for manifest_path in manifest_paths:
        try:
            manifest = download_required_json(bronze, manifest_path)
            snapshot_id = manifest["snapshot_id"]
            event_id = str(manifest["event_id"])
            fi = str(manifest["fi"])
            marker_path = f"cricket/control/processed_snapshots/{snapshot_id}_{event_id}_{fi}.json"
            if blob_exists(silver, marker_path):
                skipped += 1
                continue
            base_path = manifest_path.removesuffix("/manifest.json")
            events_inplay_payload = download_required_json(bronze, f"{base_path}/events_inplay.json")
            bet365_event_payload = download_required_json(bronze, f"{base_path}/bet365_event.json")
            event_odds_payload = download_required_json(bronze, f"{base_path}/event_odds.json")
            parsed = parse_silver_snapshot(manifest, events_inplay_payload, bet365_event_payload, event_odds_payload)
            write_silver_outputs(silver, parsed)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to parse bronze snapshot to silver")

    logging.info(json.dumps({
        "event": "parse_cricket_bronze_to_silver_completed",
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "checked": len(manifest_paths),
    }))


# -----------------------------
# Gold serving layer
# -----------------------------

def list_latest_silver_match_snapshots(silver_container, limit: int) -> List[str]:
    prefix = "cricket/inplay/"
    snapshots = []
    for blob in silver_container.list_blobs(name_starts_with=prefix):
        name = blob.name
        if name.endswith("/match_snapshot.json"):
            snapshots.append((getattr(blob, "last_modified", None), name))
    snapshots.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return [name for _, name in snapshots[:limit]]


def base_path_from_match_snapshot_path(match_snapshot_path: str) -> str:
    return match_snapshot_path.removesuffix("/match_snapshot.json")


def build_match_page(
    match_snapshot: Dict[str, Any],
    team_scores: Dict[str, Any],
    player_entries: Dict[str, Any],
    odds_records: Dict[str, Any],
    current_markets: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    teams = team_scores.get("rows", []) if isinstance(team_scores, dict) else []
    players = player_entries.get("rows", []) if isinstance(player_entries, dict) else []
    odds = odds_records.get("rows", []) if isinstance(odds_records, dict) else []
    markets = current_markets.get("rows", []) if isinstance(current_markets, dict) else []
    unique_market_ids = set(m.get("market_id") for m in markets if m.get("market_id"))
    unique_market_names = set(m.get("market_name") for m in markets if m.get("market_name"))
    return {
        "generated_at_utc": utc_now().isoformat(),
        "snapshot": {
            "snapshot_id": match_snapshot.get("snapshot_id"),
            "snapshot_time_utc": match_snapshot.get("snapshot_time_utc"),
            "event_id": match_snapshot.get("event_id"),
            "fi": match_snapshot.get("fi"),
        },
        "match_header": {
            "league_name": match_snapshot.get("league_name"),
            "match_name": match_snapshot.get("match_name"),
            "home_team": {"id": match_snapshot.get("home_team_id"), "name": match_snapshot.get("home_team_name")},
            "away_team": {"id": match_snapshot.get("away_team_id"), "name": match_snapshot.get("away_team_name")},
            "time_status": match_snapshot.get("time_status"),
        },
        "score": {
            "summary_from_events": match_snapshot.get("score_summary_events"),
            "summary_from_bet365": match_snapshot.get("score_summary_bet365"),
            "team_scores": teams,
        },
        "players": players,
        "odds": {"count": len(odds), "records": odds},
        "current_markets": {
            "selection_count": len(markets),
            "market_count": len(unique_market_ids or unique_market_names),
            "records": markets,
        },
        "source": {
            "silver_match_snapshot_path": match_snapshot.get("source_silver_match_snapshot_path"),
            "bronze_manifest_path": match_snapshot.get("source_bronze_manifest_path"),
        },
    }


def write_gold_match_page(gold_container, match_page: Dict[str, Any]) -> None:
    event_id = str(match_page["snapshot"]["event_id"])
    snapshot_id = str(match_page["snapshot"]["snapshot_id"])
    snapshot_time = match_page["snapshot"].get("snapshot_time_utc")
    dt = datetime.fromisoformat(snapshot_time.replace("Z", "+00:00")) if snapshot_time else utc_now()
    history_base = (
        f"cricket/matches/history/event_id={event_id}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}/snapshot_id={snapshot_id}"
    )
    latest_base = f"cricket/matches/latest/event_id={event_id}"
    upload_json(gold_container, f"{history_base}/match_page.json", match_page, overwrite=True)
    upload_json(gold_container, f"{latest_base}/match_page.json", match_page, overwrite=True)


def write_gold_index(gold_container, pages: List[Dict[str, Any]]) -> None:
    matches = []
    for page in pages:
        matches.append({
            "event_id": page.get("snapshot", {}).get("event_id"),
            "fi": page.get("snapshot", {}).get("fi"),
            "snapshot_id": page.get("snapshot", {}).get("snapshot_id"),
            "snapshot_time_utc": page.get("snapshot", {}).get("snapshot_time_utc"),
            "league_name": page.get("match_header", {}).get("league_name"),
            "match_name": page.get("match_header", {}).get("match_name"),
            "home_team_name": page.get("match_header", {}).get("home_team", {}).get("name"),
            "away_team_name": page.get("match_header", {}).get("away_team", {}).get("name"),
            "score_summary": page.get("score", {}).get("summary_from_events") or page.get("score", {}).get("summary_from_bet365"),
            "odds_count": page.get("odds", {}).get("count"),
            "current_market_count": page.get("current_markets", {}).get("market_count"),
            "current_market_selection_count": page.get("current_markets", {}).get("selection_count"),
            "latest_gold_path": f"gold/cricket/matches/latest/event_id={page.get('snapshot', {}).get('event_id')}/match_page.json",
        })
    index_payload = {"generated_at_utc": utc_now().isoformat(), "match_count": len(matches), "matches": matches}
    upload_json(gold_container, "cricket/matches/latest/index.json", index_payload, overwrite=True)


@app.timer_trigger(schedule="*/30 * * * * *", arg_name="timer", run_on_startup=False, use_monitor=False)
def build_cricket_gold_match_pages(timer: func.TimerRequest) -> None:
    max_events = get_int_env("MAX_GOLD_EVENTS_PER_RUN", 20)
    silver = get_named_container_client("silver")
    gold = get_named_container_client("gold")
    match_snapshot_paths = list_latest_silver_match_snapshots(silver, max_events)
    built_pages = []
    processed = failed = 0

    for match_snapshot_path in match_snapshot_paths:
        try:
            base_path = base_path_from_match_snapshot_path(match_snapshot_path)
            match_snapshot = download_required_json(silver, f"{base_path}/match_snapshot.json")
            match_snapshot["source_silver_match_snapshot_path"] = f"silver/{base_path}/match_snapshot.json"
            team_scores = download_required_json(silver, f"{base_path}/team_scores.json")
            player_entries = download_required_json(silver, f"{base_path}/player_entries.json")
            odds_records = download_required_json(silver, f"{base_path}/odds_records.json")
            try:
                current_markets = download_required_json(silver, f"{base_path}/current_markets.json")
            except ResourceNotFoundError:
                current_markets = {"rows": []}
            match_page = build_match_page(match_snapshot, team_scores, player_entries, odds_records, current_markets)
            write_gold_match_page(gold, match_page)
            built_pages.append(match_page)
            processed += 1
        except Exception:
            failed += 1
            logging.exception("Failed to build gold match page")

    if built_pages:
        write_gold_index(gold, built_pages)

    logging.info(json.dumps({
        "event": "build_cricket_gold_match_pages_completed",
        "processed": processed,
        "failed": failed,
        "checked": len(match_snapshot_paths),
    }))

@app.route(route="matches", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_latest_matches(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")
        data = download_required_json(gold, "cricket/matches/latest/index.json")
        return func.HttpResponse(json.dumps(data), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get latest matches")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)


@app.route(route="matches/{event_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_page(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")
        return func.HttpResponse(json.dumps(data), status_code=200, mimetype="application/json")
    except Exception as ex:
        logging.exception("Failed to get match page")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)
@app.route(route="matches/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_matches_list_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        gold = get_named_container_client("gold")

        latest_by_event = {}

        for blob in gold.list_blobs(name_starts_with="cricket/matches/history/"):
            name = blob.name
            if not name.endswith("/match_page.json"):
                continue

            try:
                page = download_required_json(gold, name)
                event_id = str(page.get("snapshot", {}).get("event_id", "-"))
                snapshot_time = page.get("snapshot", {}).get("snapshot_time_utc") or ""

                current = latest_by_event.get(event_id)
                current_time = current.get("snapshot_time_utc") if current else ""

                if current is None or snapshot_time > current_time:
                    latest_by_event[event_id] = {
                        "event_id": event_id,
                        "snapshot_time_utc": snapshot_time,
                        "match_name": page.get("match_header", {}).get("match_name") or "-",
                        "league_name": page.get("match_header", {}).get("league_name") or "-",
                        "score_summary": (
                            page.get("score", {}).get("summary_from_events")
                            or page.get("score", {}).get("summary_from_bet365")
                            or "-"
                        ),
                        "markets": page.get("current_markets", {}).get("market_count", 0),
                    }

            except Exception:
                logging.exception(f"Failed reading blob: {name}")

        matches = list(latest_by_event.values())
        matches.sort(key=lambda x: x.get("snapshot_time_utc"), reverse=True)

        rows = ""
        for m in matches:
            rows += f"""
            <tr>
                <td>{m["event_id"]}</td>
                <td>{m["match_name"]}</td>
                <td>{m["league_name"]}</td>
                <td>{m["score_summary"]}</td>
                <td>{m["markets"]}</td>
                <td>{m["snapshot_time_utc"]}</td>
                <td><a href="/api/matches/{m["event_id"]}/view">Open</a></td>
            </tr>
            """

        html = f"""
        <html>
        <head>
            <title>All Cricket Matches</title>
            <style>
                body {{ font-family: Arial; background:#f7f7f7; padding:30px; }}
                table {{ width:100%; border-collapse: collapse; background:white; }}
                th, td {{ padding:10px; border-bottom:1px solid #ddd; }}
                th {{ background:#222; color:white; }}
                a {{ color:#0066cc; font-weight:bold; }}
            </style>
        </head>
        <body>
            <h1>All Matches ({len(matches)})</h1>
            <table>
                <tr>
                    <th>Event</th>
                    <th>Match</th>
                    <th>League</th>
                    <th>Score</th>
                    <th>Markets</th>
                    <th>Updated</th>
                    <th></th>
                </tr>
                {rows}
            </table>
        </body>
        </html>
        """

        return func.HttpResponse(html, mimetype="text/html")

    except Exception as ex:
        logging.exception("Failed matches list page")
        return func.HttpResponse(str(ex), status_code=500)


@app.route(route="matches/{event_id}/view", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def get_match_page_html(req: func.HttpRequest) -> func.HttpResponse:
    try:
        event_id = req.route_params.get("event_id")
        gold = get_named_container_client("gold")
        data = download_required_json(gold, f"cricket/matches/latest/event_id={event_id}/match_page.json")

        header = data.get("match_header", {})
        score = data.get("score", {})
        odds = data.get("odds", {}).get("records", [])
        current_markets = data.get("current_markets", {})
        current_market_rows = current_markets.get("records", []) if isinstance(current_markets, dict) else []

        match_name = header.get("match_name") or (
            f"{header.get('home_team', {}).get('name')} vs {header.get('away_team', {}).get('name')}"
            if header.get("home_team", {}).get("name") and header.get("away_team", {}).get("name") else "Match"
        )
        score_text = score.get("summary_from_events") or score.get("summary_from_bet365") or "-"

        current_market_names = sorted({m.get("market_group_name") or "Unknown Market" for m in current_market_rows})
        market_options = '<option value="ALL">All current markets</option>'
        for market_name in current_market_names:
            safe_value = str(market_name).replace('"', '&quot;')
            market_options += f'<option value="{safe_value}">{market_name}</option>'

        current_market_table_rows = ""
        for m in current_market_rows:
            market_group_name = m.get("market_group_name") or "Unknown Market"
            market_name = m.get("market_name") or market_group_name
            odds_display = (
                f"{m.get('odds_decimal')} <span style='color:#777;'>({m.get('odds_fractional') or m.get('odds')})</span>"
                if m.get("odds_decimal") is not None
                else (m.get("odds_fractional") or m.get("odds") or "-")
            )
            current_market_table_rows += f"""
            <tr data-current-market="{str(market_group_name).replace('"', '&quot;')}">
                <td>{market_group_name}</td>
                <td>{market_name}</td>
                <td>{m.get('display_selection_name') or m.get('selection_name') or '-'}</td>
                <td>{odds_display}</td>
                <td>{m.get('handicap') or '-'}</td>
                <td>{'Yes' if m.get('suspended') else 'No'}</td>
            </tr>
            """

        odds_rows = ""
        for o in odds[:1000]:
            odds_rows += f"""
            <tr>
                <td>{o.get('score_from_odds') or o.get('score_from_match_snapshot') or '-'}</td>
                <td>{o.get('market_key') or '-'}</td>
                <td>{o.get('home_odds') or '-'}</td>
                <td>{o.get('away_odds') or '-'}</td>
                <td>{o.get('draw_odds') or '-'}</td>
                <td>{o.get('handicap') or '-'}</td>
                <td>{format_unix_ts(o.get('add_time'))}</td>
            </tr>
            """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{match_name}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 30px; background: #f7f7f7; }}
                h1 {{ margin-bottom: 5px; }}
                .score {{ font-size: 22px; margin-bottom: 25px; }}
                .cards {{ display: flex; gap: 15px; margin-bottom: 25px; flex-wrap: wrap; }}
                .card {{ background: white; padding: 18px; border-radius: 10px; box-shadow: 0 2px 8px #ddd; min-width: 160px; }}
                .label {{ color: #666; font-size: 13px; }}
                .value {{ font-size: 24px; font-weight: bold; }}
                .section {{ margin-top: 35px; }}
                select {{ padding: 10px; font-size: 16px; margin-bottom: 15px; }}
                table {{ width: 100%; border-collapse: collapse; background: white; }}
                th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
                th {{ background: #222; color: white; position: sticky; top: 0; }}
            </style>
        </head>
        <body>
            <h1>{match_name}</h1>
            <div class="score">Score: {score_text}</div>

            <div class="cards">
                <div class="card"><div class="label">Current Markets</div><div class="value">{current_markets.get('market_count', 0)}</div></div>
                <div class="card"><div class="label">Current Selections</div><div class="value">{current_markets.get('selection_count', 0)}</div></div>
                <div class="card"><div class="label">Odds History Rows</div><div class="value">{len(odds)}</div></div>
                <div class="card"><div class="label">Event ID</div><div class="value" style="font-size:16px;">{event_id}</div></div>
            </div>

            <div class="section">
                <h2>Current Bet365 Markets</h2>
                <label><b>Filter market:</b></label><br>
                <select id="currentMarketFilter" onchange="filterCurrentMarket()">{market_options}</select>
                <table>
                    <thead><tr><th>Group</th><th>Market</th><th>Selection</th><th>Odds Decimal (Fractional)</th><th>Line / Handicap</th><th>Suspended</th></tr></thead>
                    <tbody>{current_market_table_rows}</tbody>
                </table>
            </div>

            <div class="section">
                <h2>Odds Timeline</h2>
                <table>
                    <thead><tr><th>Score</th><th>Market Key</th><th>Home</th><th>Away</th><th>Draw</th><th>Handicap</th><th>Time</th></tr></thead>
                    <tbody>{odds_rows}</tbody>
                </table>
            </div>

            <script>
                function filterCurrentMarket() {{
                    const selected = document.getElementById("currentMarketFilter").value;
                    const rows = document.querySelectorAll("tr[data-current-market]");
                    rows.forEach(row => {{
                        const market = row.getAttribute("data-current-market");
                        row.style.display = selected === "ALL" || market === selected ? "" : "none";
                    }});
                }}
            </script>
        </body>
        </html>
        """
        return func.HttpResponse(html, status_code=200, mimetype="text/html")
    except Exception as ex:
        logging.exception("Failed to render match HTML")
        return func.HttpResponse(f"Error: {str(ex)}", status_code=500)
